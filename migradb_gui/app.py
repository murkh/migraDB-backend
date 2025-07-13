"""Entry point for the MigraDB GUI application.

This initial scaffold creates a minimal PySide6 window with a menu bar and a
central widget placeholder. More complex widgets (connection manager, schema
explorer, etc.) will be added in subsequent iterations.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QStatusBar,
    QMenu,
    QMenuBar,
    QMessageBox,
    QDialog,
)

from migradb_gui.connection import ConnectionDialog, ConnectionPair
from migradb_gui.home import HomeWidget

APP_NAME = "MigraDB GUI"


class MainWindow(QMainWindow):
    """Main application window."""

    def _open_transform_editor(self) -> None:  # noqa: D401
        from migradb_gui.transform_editor import TransformEditorDialog

        dlg = TransformEditorDialog(self)
        dlg.exec()

    def _open_connections(self) -> None:
        """Open the connection dialog and store validated connections."""
        dlg = ConnectionDialog(self)
        if dlg.exec() == QDialog.Accepted:
            pair = dlg.pair
            if pair:
                self._connections = pair
                self.statusBar().showMessage(
                    f"Connected: {pair.source.database} → {pair.target.database}")
                # show schema explorer
                # local import to avoid heavy deps if unused
                from migradb_gui.explorer import SchemaExplorer

                explorer = SchemaExplorer(pair, self)
                self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, explorer)
            else:
                QMessageBox.warning(self, "Connections",
                                    "No connections returned.")
        else:
            QMessageBox.warning(self, "Connections",
                                "No connections returned.")

    """Main application window."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(900, 700)

        # Home landing widget
        home = HomeWidget()
        home.start_migration_requested.connect(self._open_connections)
        self.setCentralWidget(home)

        # Status bar
        self.setStatusBar(QStatusBar())
        self.statusBar().showMessage("Ready")

        # Connection storage
        self._connections: ConnectionPair | None = None

        # Menu bar
        menubar = self.menuBar()
        db_menu = menubar.addMenu("Database")
        conn_action = db_menu.addAction("Connections…")
        conn_action.triggered.connect(self._open_connections)

        # Transform editor
        edit_action = db_menu.addAction("Edit Transforms…")
        edit_action.triggered.connect(self._open_transform_editor)


def run(argv: Optional[list[str]] | None = None) -> None:  # pragma: no cover
    """Run the application.

    The `migradb-gui` entry-point defined in *pyproject.toml* resolves to this
    function.
    """

    app = QApplication(argv or sys.argv)
    app.setApplicationDisplayName(APP_NAME)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":  # allow `python -m migradb_gui.app`
    run()
