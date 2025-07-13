"""Schema Explorer dock widget for MigraDB GUI.

Displays source and target PostgreSQL schemas side-by-side using `QTreeWidget`.
Tables whose primary key type differs (int→UUID) are highlighted and contain a
checkbox to choose automatic UUID conversion during migration.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

from PySide6.QtCore import Qt
from PySide6.QtGui import QBrush, QColor
from PySide6.QtWidgets import (
    QDockWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QWidget,
    QVBoxLayout,
    QPushButton,
    QDialog,
)

from migradb_gui.runner import RunnerDialog
from migradb_gui.mapper import MappingDialog
from migradb_gui.storage import load_mappings, save_mappings

from migradb_gui.schema import ColumnInfo, SchemaInspector, TableInfo
from migradb_gui.connection import ConnectionPair

_HIGHLIGHT_BRUSH = QBrush(QColor("orange"))


class SchemaExplorer(QDockWidget):
    """Dock widget showing old vs. new schemas."""

    def __init__(self, pair: ConnectionPair, parent: QWidget | None = None) -> None:
        super().__init__("Schema Explorer", parent)
        self._pair = pair

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Table / Column", "Type", "Notes"])
        self.tree.setColumnWidth(0, 200)

        run_btn = QPushButton("Run Migration")
        run_btn.clicked.connect(self._on_run)

        container = QWidget()
        vbox = QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(self.tree)
        vbox.addWidget(run_btn)

        self.setWidget(container)

        self._column_maps: dict[str, dict[str, str]] = load_mappings(pair)
        self.tree.itemDoubleClicked.connect(self._on_double_click)

        self._load_schemas()
        # mark mapped tables
        for i in range(self.tree.topLevelItemCount()):
            tbl_item = self.tree.topLevelItem(i)
            if tbl_item.text(0) in self._column_maps:
                tbl_item.setText(2, "Columns mapped")

    # ------------------------------------------------------------------
    # slots
    # ------------------------------------------------------------------

    def _on_run(self) -> None:  # noqa: D401
        uuid_tables = {self.tree.topLevelItem(i).text(0)
                       for i in range(self.tree.topLevelItemCount())
                       if self.tree.topLevelItem(i).checkState(0) == Qt.CheckState.Checked}
        dlg = RunnerDialog(self._pair, uuid_tables, self._column_maps, self)
        dlg.exec()

    def _on_double_click(self, item: QTreeWidgetItem, col: int) -> None:  # noqa: D401
        # only allow mapping for table-level items (no parent)
        if item.parent() is not None:
            return
        table_name = item.text(0)
        existing = self._column_maps.get(table_name, {})
        dlg = MappingDialog(self._pair, table_name, existing, self)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.mapping is not None:
            self._column_maps[table_name] = dlg.mapping
            item.setText(2, "Columns mapped")
            save_mappings(self._pair, self._column_maps)

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _load_schemas(self) -> None:  # noqa: D401
        self.tree.clear()

        src = SchemaInspector(self._pair.source.sqlalchemy_url())
        tgt = SchemaInspector(self._pair.target.sqlalchemy_url())

        src_tables = {t.name: t for t in src.list_tables()}
        tgt_tables = {t.name: t for t in tgt.list_tables()}

        table_names = sorted(set(src_tables) | set(tgt_tables))
        for name in table_names:
            src_tbl = src_tables.get(name)
            tgt_tbl = tgt_tables.get(name)
            root_item = QTreeWidgetItem(self.tree, [name])

            if src_tbl and tgt_tbl:
                self._populate_table(root_item, src_tbl, tgt_tbl)
            elif src_tbl:
                root_item.setForeground(0, _HIGHLIGHT_BRUSH)
                root_item.setText(2, "Missing in target")
                self._populate_columns_only(root_item, src_tbl)
            elif tgt_tbl:
                root_item.setForeground(0, _HIGHLIGHT_BRUSH)
                root_item.setText(2, "Missing in source")
                self._populate_columns_only(root_item, tgt_tbl)

    def _populate_columns_only(self, parent: QTreeWidgetItem, tbl: TableInfo) -> None:
        for col in tbl.columns:
            QTreeWidgetItem(parent, [f"{col.name}", col.data_type])

    def _populate_table(self, parent: QTreeWidgetItem, src_tbl: TableInfo, tgt_tbl: TableInfo) -> None:
        src_pk = src_tbl.primary_keys[0] if src_tbl.primary_keys else None
        tgt_pk = tgt_tbl.primary_keys[0] if tgt_tbl.primary_keys else None
        needs_uuid_conv = src_pk and tgt_pk and src_pk.is_incremental_pk() and tgt_pk.is_uuid_pk()

        if needs_uuid_conv:
            parent.setCheckState(0, Qt.CheckState.Checked)
            parent.setText(2, "PK int→UUID")
            parent.setForeground(0, _HIGHLIGHT_BRUSH)
        else:
            parent.setCheckState(0, Qt.CheckState.Unchecked)

        col_names = {c.name for c in src_tbl.columns} | {
            c.name for c in tgt_tbl.columns}
        for col_name in sorted(col_names):
            src_col = next(
                (c for c in src_tbl.columns if c.name == col_name), None)
            tgt_col = next(
                (c for c in tgt_tbl.columns if c.name == col_name), None)
            item = QTreeWidgetItem(parent, [col_name])
            if src_col and tgt_col:
                type_str = f"{src_col.data_type} → {tgt_col.data_type}" if src_col.data_type != tgt_col.data_type else src_col.data_type
                item.setText(1, type_str)
                if src_col.data_type != tgt_col.data_type:
                    item.setForeground(1, _HIGHLIGHT_BRUSH)
            elif src_col:
                item.setText(1, src_col.data_type)
                item.setText(2, "Missing in target")
            elif tgt_col:
                item.setText(1, tgt_col.data_type)
                item.setText(2, "Missing in source")
