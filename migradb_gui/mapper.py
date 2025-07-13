"""Column mapping dialog."""
from __future__ import annotations

from typing import Dict, List

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QMessageBox,
    QWidget,
)

from migradb_gui.schema import SchemaInspector
from migradb_gui.connection import ConnectionPair


class MappingDialog(QDialog):
    """Dialog allowing user to map source → target columns for a table."""

    def __init__(
        self,
        pair: ConnectionPair,
        table_name: str,
        existing_map: Dict[str, str] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"Map Columns – {table_name}")
        self._table_name = table_name
        self._pair = pair
        self._combos: Dict[str, QComboBox] = {}

        src_cols = {c.name for c in SchemaInspector(pair.source.sqlalchemy_url()).list_tables() if c.name == table_name}.pop()

        # get columns lists
        src_inspector = SchemaInspector(pair.source.sqlalchemy_url())
        tgt_inspector = SchemaInspector(pair.target.sqlalchemy_url())
        src_tbl = next(t for t in src_inspector.list_tables() if t.name == table_name)
        tgt_tbl = next(t for t in tgt_inspector.list_tables() if t.name == table_name)

        src_names = [c.name for c in src_tbl.columns]
        tgt_names = [c.name for c in tgt_tbl.columns]

        layout = QFormLayout()
        for tgt in tgt_names:
            combo = QComboBox()
            combo.addItem("<ignore>")
            combo.addItems(src_names)
            if existing_map and tgt in existing_map:
                idx = combo.findText(existing_map[tgt])
                if idx >= 0:
                    combo.setCurrentIndex(idx)
            layout.addRow(QLabel(tgt), combo)
            self._combos[tgt] = combo

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)
        self.setLayout(layout)

        self._result_map: Dict[str, str] | None = None

    # ------------------------------------------------------------------

    def _on_accept(self) -> None:
        mapping: Dict[str, str] = {}
        for tgt, combo in self._combos.items():
            src = combo.currentText()
            if src != "<ignore>":
                mapping[tgt] = src
        if not mapping:
            if QMessageBox.question(self, "No Mapping", "No columns mapped – continue?", QMessageBox.Yes | QMessageBox.No) == QMessageBox.No:
                return
        self._result_map = mapping
        self.accept()

    @property
    def mapping(self) -> Dict[str, str] | None:  # noqa: D401
        return self._result_map
