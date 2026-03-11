"""Testes do gerador de relatório PDF.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa do gerador de relatório PDF.
"""

from __future__ import annotations

from gap_tributario.report.pdf import PDFReport


def test_pdf_report_importable():
    """Verifica que o gerador PDF é importável."""
    assert PDFReport is not None


def test_pdf_report_instantiable():
    """Verifica que o gerador PDF pode ser instanciado."""
    report = PDFReport()
    assert report is not None
