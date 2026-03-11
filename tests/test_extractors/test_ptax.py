"""Testes do extrator BCB PTAX.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa de extrator BCB PTAX.
"""

from __future__ import annotations

from gap_tributario.extractors.ptax import PTAXExtractor


def test_ptax_extractor_importable():
    """Verifica que o extrator é importável."""
    assert PTAXExtractor is not None


def test_ptax_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado com defaults."""
    extractor = PTAXExtractor()
    assert extractor.timeout == 15
    assert extractor.max_retries == 3
