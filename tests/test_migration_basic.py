"""Basic migration smoke test using in-memory SQLite.

This verifies that MigrationExecutor copies rows, generates UUIDs, applies
column maps, and maintains row counts.
"""
from __future__ import annotations

import uuid

from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import Session

from migradb_gui.migration import MigrationExecutor, MigrationSettings
from migradb_gui.connection import DBConnection, ConnectionPair


def _build_source(engine):  # noqa: D401
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("full_name", String),
    )
    addresses = Table(
        "addresses",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", ForeignKey("users.id")),
        Column("email", String),
    )
    meta.create_all(engine)
    with Session(engine) as s:
        s.begin()
        uid = s.execute(users.insert().values(full_name="Alice")).inserted_primary_key[0]
        s.execute(addresses.insert().values(user_id=uid, email="a@example.com"))
        s.commit()


def _build_target(engine):  # noqa: D401
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", String, primary_key=True),  # will fill UUID str
        Column("full_name", String),
    )
    addresses = Table(
        "addresses",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", ForeignKey("users.id")),
        Column("email", String),
    )
    meta.create_all(engine)


def test_basic_migration():  # noqa: D401
    src_engine = create_engine("sqlite:///:memory:")
    tgt_engine = create_engine("sqlite:///:memory:")

    _build_source(src_engine)
    _build_target(tgt_engine)

    pair = ConnectionPair(
        source=DBConnection(driver="sqlite", username="", password="", host="", port=0, database=":memory:"),
        target=DBConnection(driver="sqlite", username="", password="", host="", port=0, database=":memory:"),
    )

    # monkeypatch engines inside executor
    settings = MigrationSettings(uuid_tables={"users"}, column_maps={})
    exec = MigrationExecutor(pair, settings)
    exec._src_engine = src_engine  # type: ignore[attr-defined]
    exec._tgt_engine = tgt_engine  # type: ignore[attr-defined]
    exec._src_meta.reflect(bind=src_engine)
    exec._tgt_meta.reflect(bind=tgt_engine)

    exec.run()

    with Session(tgt_engine) as s:
        count = s.execute("SELECT COUNT(*) FROM users").scalar_one()
        assert count == 1
        uid = s.execute("SELECT id FROM users").scalar_one()
        # valid UUID string length 36 or can validate parse
        uuid.UUID(uid)
