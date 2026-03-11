"""Schemas Pandera para validação de DataFrames por fonte de dados.

Implementa validação fail-fast: o primeiro erro aborta o pipeline.

Schemas definidos:
- ArrecadacaoSchema: valida dados do GFIS2 Parquet
- ComexSchema: valida dados do MDIC ComEx CSV
- IBGESchema: valida dados da API IBGE SIDRA
- PTAXSchema: valida dados da API BCB PTAX
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar validação de dados com Pandera schemas fail-fast"


class ArrecadacaoSchema:
    """Schema Pandera para validação dos dados de arrecadação GFIS2."""

    pass


class ComexSchema:
    """Schema Pandera para validação dos dados MDIC ComEx."""

    pass


class IBGESchema:
    """Schema Pandera para validação dos dados IBGE SIDRA."""

    pass


class PTAXSchema:
    """Schema Pandera para validação dos dados BCB PTAX."""

    pass
