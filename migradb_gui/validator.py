"""Validation utilities for MigraDB."""
from __future__ import annotations

import hashlib
from typing import Dict, List

from sqlalchemy import Table, select, func
from sqlalchemy.orm import Session


def compare_constraints(src_tbl: Table, tgt_tbl: Table) -> List[str]:  # noqa: D401
    """Return list of discrepancies between source and target constraints."""
    issues: List[str] = []
    if len(src_tbl.primary_key.columns) != len(tgt_tbl.primary_key.columns):
        issues.append("Primary key column count mismatch")

    # not-null columns
    src_nn = {c.name for c in src_tbl.columns if not c.nullable}
    tgt_nn = {c.name for c in tgt_tbl.columns if not c.nullable}
    if src_nn != tgt_nn:
        issues.append(f"NOT NULL columns differ: src={src_nn ^ tgt_nn}")

    # FK names sets
    src_fk = {fk.column.table.name for fk in src_tbl.foreign_keys}
    tgt_fk = {fk.column.table.name for fk in tgt_tbl.foreign_keys}
    if src_fk - tgt_fk:
        issues.append(f"Missing FKs in target: {src_fk - tgt_fk}")
    return issues


def column_checksums(session: Session, tbl: Table, batch_size: int = 1000) -> Dict[str, str]:  # noqa: D401
    """Compute MD5 checksum per column by concatenating values (simple)."""
    col_checks: Dict[str, hashlib._hashlib.HASH] = {c.name: hashlib.md5() for c in tbl.columns}
    pk_col = list(tbl.primary_key.columns)[0]
    last_pk = None
    while True:
        q = select(tbl).order_by(pk_col).limit(batch_size)
        if last_pk is not None:
            q = q.where(pk_col > last_pk)
        rows = session.execute(q).all()
        if not rows:
            break
        for row in rows:
            mapping = row._mapping
            last_pk = mapping[pk_col.name]
            for name, value in mapping.items():
                col_checks[name].update(repr(value).encode())
    return {k: v.hexdigest() for k, v in col_checks.items()}
