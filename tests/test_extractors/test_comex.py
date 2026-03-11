"""Testes do extrator MDIC ComEx.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa de extrator MDIC ComEx.
"""

from __future__ import annotations

from gap_tributario.extractors.comex import ComexExtractor


def test_comex_extractor_importable():
    """Verifica que o extrator é importável."""
    assert ComexExtractor is not None


def test_comex_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado."""
    extractor = ComexExtractor(mdic_base_path="./test_path")
    assert extractor.mdic_base_path == "./test_path"
