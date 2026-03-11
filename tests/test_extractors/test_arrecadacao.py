"""Testes do extrator de arrecadação GFIS2 Parquet.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa de extrator GFIS2.
"""

from __future__ import annotations

from gap_tributario.extractors.arrecadacao import ArrecadacaoExtractor


def test_arrecadacao_extractor_importable():
    """Verifica que o extrator é importável."""
    assert ArrecadacaoExtractor is not None


def test_arrecadacao_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado."""
    extractor = ArrecadacaoExtractor(parquet_base_path="./test_path")
    assert extractor.parquet_base_path == "./test_path"
