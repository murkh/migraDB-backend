"""Home (landing) widget for MigraDB GUI."""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QLabel, QPushButton, QVBoxLayout, QWidget


class HomeWidget(QWidget):
    """Landing page with prominent action button."""

    start_migration_requested = Signal()

    def __init__(self) -> None:
        super().__init__()

        title = QLabel("<h1>New Migration</h1>")
        subtitle = QLabel("Configure your migration from an old schema to a new schema.")
        subtitle.setWordWrap(True)

        start_btn = QPushButton("Start Migration")
        start_btn.setFixedWidth(160)
        start_btn.clicked.connect(self.start_migration_requested.emit)  # type: ignore[arg-type]

        layout = QVBoxLayout(spacing=20)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignHCenter)
        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addWidget(start_btn, alignment=Qt.AlignmentFlag.AlignLeft)
        layout.addStretch()

        self.setLayout(layout)
