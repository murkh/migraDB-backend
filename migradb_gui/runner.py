"""Migration runner dialog with progress bar and live log."""
from __future__ import annotations

import threading
from typing import List, Set

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QProgressBar,
    QTextEdit,
    QVBoxLayout,
)

from migradb_gui.connection import ConnectionPair
from migradb_gui.migration import MigrationExecutor, MigrationSettings


class RunnerDialog(QDialog):
    """Dialog that executes migration in background thread."""

    def __init__(self, pair: ConnectionPair, uuid_tables: Set[str], column_maps: dict[str, dict[str, str]], parent=None) -> None:
        super().__init__(parent)
        self._column_maps = column_maps
        self.setWindowTitle("Running Migration")
        self.resize(600, 400)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.log = QTextEdit(readOnly=True)

        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btn_box.rejected.connect(self.reject)
        btn_box.button(QDialogButtonBox.StandardButton.Close).setEnabled(False)
        self._close_btn = btn_box.button(QDialogButtonBox.StandardButton.Close)

        layout = QVBoxLayout()
        layout.addWidget(self.progress)
        layout.addWidget(self.log)
        layout.addWidget(btn_box)
        self.setLayout(layout)

        # start worker thread
        threading.Thread(
            target=self._run_exec,
            args=(pair, uuid_tables, column_maps),
            daemon=True,
        ).start()

    # ------------------------------------------------------------------

    def _run_exec(self, pair: ConnectionPair, uuid_tables: Set[str], column_maps: dict[str, dict[str, str]]) -> None:
        total_tables = len(uuid_tables)
        processed_tables: int = 0

        def progress_cb(tbl: str, done: int, total: int) -> None:  # noqa: D401
            self.log.append(f"{tbl}: {done}/{total}")

        exec = MigrationExecutor(pair, MigrationSettings(uuid_tables=uuid_tables, column_maps=column_maps), progress_cb)
        try:
            exec.run()
            self.log.append("\nMigration successful ✅")
        except Exception as exc:
            self.log.append(f"\nMigration failed: {exc}")
        self.progress.setValue(100)
        self._close_btn.setEnabled(True)
