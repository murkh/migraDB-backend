"""Data migration engine for MigraDB.

This module performs the core migration steps:

* Builds dependency order between tables (topological sort on FK graph).
* For tables flagged for *UUID conversion*, generates new UUID primary keys and
  tracks an in-memory mapping old_id → new_uuid so child tables can remap
  foreign keys.
* Copies data in batches, wrapped in a single transaction (per run) so errors
  trigger rollback.
* Emits progress events via an optional callback to update GUI progress bars.
* Uses *rich* logging for colorful console logging.
"""
from __future__ import annotations

import logging
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Dict, List, Set, Tuple

from rich.logging import RichHandler
from sqlalchemy import MetaData, Table, create_engine, select, func
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from importlib import import_module

from migradb_gui.connection import ConnectionPair

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(RichHandler(markup=True))

ProgressCallback = Callable[[str, int, int], None]


@dataclass
class MigrationSettings:
    """User choices for the migration run."""

    uuid_tables: Set[str]  # tables needing int→UUID conversion
    # target_col -> source_col per table
    column_maps: Dict[str, Dict[str, str]]
    batch_size: int = 1000
    validate: bool = True


class MigrationExecutor:
    """Executes migration based on provided settings."""

    def __init__(
        self,
        pair: ConnectionPair,
        settings: MigrationSettings,
        progress_cb: ProgressCallback | None = None,
    ) -> None:
        self._pair = pair
        self._settings = settings
        self._progress_cb = progress_cb or (lambda *_: None)

        self._src_engine: Engine = create_engine(
            pair.source.sqlalchemy_url(), future=True)
        self._tgt_engine: Engine = create_engine(
            pair.target.sqlalchemy_url(), future=True)

        self._src_meta = MetaData()
        self._tgt_meta = MetaData()

        # optional transforms module
        try:
            self._transforms = import_module("migradb_gui.transforms")
        except ModuleNotFoundError:
            self._transforms = None
        self._src_meta.reflect(bind=self._src_engine)
        self._tgt_meta.reflect(bind=self._tgt_engine)

        self._id_maps: Dict[str, Dict[int, uuid.UUID]] = defaultdict(dict)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def run(self) -> None:  # noqa: D401
        logger.info("[bold cyan]Starting migration[/bold cyan]")
        order = self._determine_order()
        logger.info("Dependency order: %s", order)

        with Session(self._tgt_engine) as tgt_sess, Session(self._src_engine) as src_sess:
            try:
                tgt_sess.begin()
                for tbl_name in order:
                    self._migrate_table(src_sess, tgt_sess, tbl_name)
                tgt_sess.commit()
                logger.info("[bold green]Migration successful[/bold green]")
            except Exception:
                logger.exception("Migration failed – rolling back")
                tgt_sess.rollback()
                raise

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _determine_order(self) -> List[str]:  # noqa: D401
        """Topologically sort tables based on FK dependencies."""
        graph: Dict[str, Set[str]] = defaultdict(set)
        for table in self._src_meta.sorted_tables:
            deps = {fk.column.table.name for fk in table.foreign_keys}
            graph[table.name].update(deps)

        # Kahn's algorithm
        in_degree = {t: 0 for t in graph}
        for deps in graph.values():
            for d in deps:
                in_degree[d] = in_degree.get(d, 0) + 1

        queue = deque([t for t, deg in in_degree.items() if deg == 0])
        order: List[str] = []
        while queue:
            n = queue.popleft()
            order.append(n)
            for m in graph[n]:
                in_degree[m] -= 1
                if in_degree[m] == 0:
                    queue.append(m)
        return order

    # ------------------------------------------------------------------

    def _migrate_table(self, src_sess: Session, tgt_sess: Session, tbl_name: str) -> None:
        src_tbl: Table = self._src_meta.tables[tbl_name]

        # check for custom split transform
        split_fn = None
        if self._transforms is not None:
            split_fn = getattr(self._transforms, f"split_{tbl_name}", None)

        if split_fn:
            self._migrate_with_split(src_sess, tgt_sess, src_tbl, split_fn)
            return

        tgt_tbl: Table = self._tgt_meta.tables[tbl_name]

        total_rows = src_sess.execute(
            select(func.count()).select_from(src_tbl)).scalar_one()
        copied = 0

        col_map = self._settings.column_maps.get(tbl_name, {})

        # iterate in batches (default path)
        pk_col = list(src_tbl.primary_key.columns)[0]
        last_pk = None
        while True:
            query = select(src_tbl).order_by(
                pk_col).limit(self._settings.batch_size)
            if last_pk is not None:
                query = query.where(pk_col > last_pk)
            rows = src_sess.execute(query).all()
            if not rows:
                break

            insert_rows = []
            for row in rows:
                row_dict = dict(row._mapping)
                last_pk = row_dict[pk_col.name]

                # apply column mapping overrides
                for tgt_col, src_col in col_map.items():
                    if src_col in row_dict:
                        row_dict[tgt_col] = row_dict[src_col]
                # handle UUID generation if needed
                if tbl_name in self._settings.uuid_tables:
                    new_uuid = uuid.uuid4()
                    self._id_maps[tbl_name][last_pk] = new_uuid
                    row_dict[pk_col.name] = new_uuid

                # remap foreign keys referencing previously converted tables
                for fk in tgt_tbl.foreign_keys:
                    ref_tbl = fk.column.table.name
                    if ref_tbl in self._id_maps:
                        old_val = row_dict[fk.parent.name]
                        if old_val in self._id_maps[ref_tbl]:
                            row_dict[fk.parent.name] = self._id_maps[ref_tbl][old_val]
                insert_rows.append(row_dict)

            # type: ignore[arg-type]
            tgt_sess.execute(tgt_tbl.insert(), insert_rows)
            copied += len(insert_rows)
            self._progress_cb(tbl_name, copied, total_rows)

        # post-copy integrity check
        if self._settings.validate:
            tgt_count = tgt_sess.execute(select(func.count()).select_from(tgt_tbl)).scalar_one()
            if tgt_count != total_rows:
                raise ValueError(f"Row count mismatch for {tbl_name}: source={total_rows} target={tgt_count}")

        # Add transform hook support
        if self._transforms is not None:
            transform_hook = getattr(self._transforms, f"transform_{tbl_name}", None)
            if transform_hook:
                transform_hook(tgt_sess, insert_rows)

        logger.info(f"{tbl_name}: copied {copied} rows")
