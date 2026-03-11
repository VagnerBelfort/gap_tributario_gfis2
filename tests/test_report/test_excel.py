"""Testes do gerador de relatório Excel.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa do gerador de relatório Excel.
"""

from __future__ import annotations

from gap_tributario.report.excel import ExcelReport


def test_excel_report_importable():
    """Verifica que o gerador Excel é importável."""
    assert ExcelReport is not None


def test_excel_report_instantiable():
    """Verifica que o gerador Excel pode ser instanciado."""
    report = ExcelReport()
    assert report is not None
