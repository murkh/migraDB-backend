"""Connection dialog and models for MigraDB GUI.

This module provides:
* `PgConnection`: a pydantic model holding PostgreSQL connection info.
* `ConnectionDialog`: a `QDialog` that lets the user enter connection details
  for *source* (old) and *target* (new) databases, test them, and emit the
  validated connection pair.

For now we only store the connection settings in memory; persistence to a YAML
config file will be added later.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Tuple

from pydantic import BaseModel, Field, SecretStr, ValidationError, field_validator
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)


class PgConnection(BaseModel):
    """PostgreSQL connection parameters."""

    host: str = Field(..., description="Hostname or IP address")
    port: int = Field(5432, description="TCP port")
    database: str = Field(..., description="Database name")
    user: str = Field(..., description="Username")
    password: SecretStr = Field(..., description="Password")

    @field_validator("host")
    @classmethod
    def non_empty(cls, v: str) -> str:  # noqa: D401, N805
        if not v:
            raise ValueError("host cannot be empty")
        return v

    def sqlalchemy_url(self) -> str:
        """Return a postgres+psycopg2 URL string."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class ConnectionPair:
    """Pair of source and target connections."""

    source: PgConnection
    target: PgConnection


class _PgForm(QWidget):
    """Reusable form widget for a single PgConnection."""

    def __init__(self, title: str) -> None:
        super().__init__()
        self._title = title

        self.host_edit = QLineEdit()
        self.port_edit = QLineEdit("5432")
        self.db_edit = QLineEdit()
        self.user_edit = QLineEdit()
        self.pwd_edit = QLineEdit()
        self.pwd_edit.setEchoMode(QLineEdit.EchoMode.Password)

        layout = QFormLayout(spacing=6)
        layout.addRow("Host", self.host_edit)
        layout.addRow("Port", self.port_edit)
        layout.addRow("Database", self.db_edit)
        layout.addRow("User", self.user_edit)
        layout.addRow("Password", self.pwd_edit)
        self.setLayout(layout)

    def to_connection(self) -> PgConnection | None:
        try:
            return PgConnection(
                host=self.host_edit.text().strip(),
                port=int(self.port_edit.text().strip() or 5432),
                database=self.db_edit.text().strip(),
                user=self.user_edit.text().strip(),
                password=self.pwd_edit.text(),
            )
        except (ValidationError, ValueError) as exc:
            QMessageBox.critical(self, self._title, str(exc))
            return None


class ConnectionDialog(QDialog):
    """Dialog to capture old (source) and new (target) database connections."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Manage Connections")
        self.resize(480, 320)
        self._pair: ConnectionPair | None = None
    """Dialog to capture old (source) and new (target) database connections."""



    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Manage Connections")
        self.resize(480, 320)

        self.source_form = _PgForm("Source DB Validation Error")
        self.target_form = _PgForm("Target DB Validation Error")

        tabs = QTabWidget()
        tabs.addTab(self.source_form, "Source (Old)")
        tabs.addTab(self.target_form, "Target (New)")

        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(self._on_accept)
        btn_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(tabs)
        layout.addWidget(btn_box)
        self.setLayout(layout)

    # ---------------------------------------------------------------------
    # properties & slots
    # ---------------------------------------------------------------------

    @property
    def pair(self) -> ConnectionPair | None:  # noqa: D401
        return self._pair

    def _on_accept(self) -> None:  # noqa: D401
        source = self.source_form.to_connection()
        if not source:
            return
        target = self.target_form.to_connection()
        if not target:
            return
        self._pair = ConnectionPair(source, target)
        self.accept()
