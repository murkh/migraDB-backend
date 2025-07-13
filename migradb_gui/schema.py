"""Schema introspection utilities for MigraDB.

This currently targets PostgreSQL via SQLAlchemy. Only minimal metadata is
retrieved (tables, columns, PK flag, data type). Future: constraints, FKs, etc.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


@dataclass
class ColumnInfo:
    """Metadata about a database column."""

    name: str
    data_type: str
    is_primary: bool

    def is_incremental_pk(self) -> bool:
        """Return *True* if the column looks like a serial/incremental PK."""
        # naive heuristics: integer type and part of pk
        return self.is_primary and self.data_type in {
            "integer",
            "bigint",
            "smallint",
        }

    def is_uuid_pk(self) -> bool:
        return self.is_primary and self.data_type == "uuid"


@dataclass
class TableInfo:
    """Metadata about a database table."""

    name: str
    columns: List[ColumnInfo]

    @property
    def primary_keys(self) -> List[ColumnInfo]:
        return [c for c in self.columns if c.is_primary]


class SchemaInspector:
    """Introspect PostgreSQL schema via SQLAlchemy."""

    def __init__(self, sqlalchemy_url: str) -> None:
        self._engine: Engine = create_engine(sqlalchemy_url, future=True)
        self._inspector = inspect(self._engine)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def list_tables(self) -> List[TableInfo]:  # noqa: D401
        tables: List[TableInfo] = []
        for table_name in self._inspector.get_table_names():
            pk_set = set(self._inspector.get_pk_constraint(table_name)["constrained_columns"])
            cols: List[ColumnInfo] = []
            for col in self._inspector.get_columns(table_name):
                cols.append(
                    ColumnInfo(
                        name=col["name"],
                        data_type=str(col["type"]),
                        is_primary=col["name"] in pk_set,
                    )
                )
            tables.append(TableInfo(name=table_name, columns=cols))
        return tables
