"""Simple transform editor dialog for MigraDB.

Allows editing of migradb_gui/transforms.py in a plain text editor with
syntax highlighting courtesy of Qt (fallback to plain text).
"""
from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QSyntaxHighlighter, QTextCharFormat, QColor, QTextDocument
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QPlainTextEdit,
    QVBoxLayout,
)

TRANSFORM_PATH = Path(__file__).with_name("transforms.py")


class _PythonHighlighter(QSyntaxHighlighter):
    """Very lightweight Python syntax highlighter."""

    def __init__(self, doc: QTextDocument) -> None:
        super().__init__(doc)
        kw_format = QTextCharFormat()
        kw_format.setForeground(QColor("blue"))
        self._rules = [(rf"\b{kw}\b", kw_format) for kw in (
            "def", "return", "import", "from", "as", "if", "else", "for", "while", "try", "except", "class", "with", "yield", "lambda", "True", "False", "None",
        )]

    def highlightBlock(self, text: str) -> None:  # noqa: D401
        for pattern, fmt in self._rules:
            i = 0
            while True:
                i = text.find(pattern.strip("\\b"), i)
                if i == -1:
                    break
                length = len(pattern.strip("\\b"))
                self.setFormat(i, length, fmt)
                i += length


class TransformEditorDialog(QDialog):
    """Plain-text editor for transforms.py."""

    def __init__(self, parent=None) -> None:  # noqa: D401
        super().__init__(parent)
        self.setWindowTitle("Edit Transform Hooks")
        self.resize(700, 600)

        self.editor = QPlainTextEdit()
        font = QFont("Courier New", 10)
        self.editor.setFont(font)
        _PythonHighlighter(self.editor.document())

        if TRANSFORM_PATH.exists():
            self.editor.setPlainText(TRANSFORM_PATH.read_text())
        else:
            # create initial template
            TRANSFORM_PATH.write_text("\n")

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._on_save)
        btns.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(self.editor)
        layout.addWidget(btns)
        self.setLayout(layout)

    # ------------------------------------------------------------------

    def _on_save(self) -> None:
        TRANSFORM_PATH.write_text(self.editor.toPlainText())
        self.accept()
