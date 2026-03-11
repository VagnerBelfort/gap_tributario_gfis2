"""Testes do extrator IBGE SIDRA.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa de extrator IBGE SIDRA.
"""

from __future__ import annotations

from gap_tributario.extractors.ibge import IBGEExtractor


def test_ibge_extractor_importable():
    """Verifica que o extrator é importável."""
    assert IBGEExtractor is not None


def test_ibge_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado com defaults."""
    extractor = IBGEExtractor()
    assert extractor.timeout == 30
    assert extractor.max_retries == 3
