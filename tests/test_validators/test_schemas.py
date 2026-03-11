"""Testes dos validators Pandera.

Scaffolding — verificam importabilidade.
Implementação completa na tarefa de validators.
"""

from __future__ import annotations

from gap_tributario.validators.schemas import (
    ArrecadacaoSchema,
    ComexSchema,
    IBGESchema,
    PTAXSchema,
)


def test_schemas_importable():
    """Verifica que todos os schemas são importáveis."""
    assert ArrecadacaoSchema is not None
    assert ComexSchema is not None
    assert IBGESchema is not None
    assert PTAXSchema is not None
