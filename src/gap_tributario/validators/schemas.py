"""Schemas Pandera para validação de DataFrames por fonte de dados.

Implementa validação fail-fast: o primeiro erro aborta o pipeline.

Schemas definidos:
- ArrecadacaoSchema: valida dados do GFIS2 Parquet (colunas reais, não TechSpec)
- ComexSchema: valida dados do MDIC ComEx CSV
- IBGESchema: valida resposta intermediária da API IBGE SIDRA
- PTAXSchema: valida resposta da API BCB PTAX
"""

from __future__ import annotations

import pandera.polars as pa
import polars as pl

# ---------------------------------------------------------------------------
# ArrecadacaoSchema — GFIS2 Parquet (camada ouro g_arrecadacao)
#
# Nomes de colunas reais (divergem do TechSpec):
#   per_aaaa      → TechSpec documenta per_nro_ano
#   val_icms_st_sda → TechSpec documenta val_icms_st
# ---------------------------------------------------------------------------
ArrecadacaoSchema = pa.DataFrameSchema(
    columns={
        "per_aaaa": pa.Column(pl.Int32, nullable=False),
        "per_nro_trimestre": pa.Column(pl.Int32, nullable=False),
        "val_icms_normal": pa.Column(pl.Float64, pa.Check.ge(0), nullable=False),
        "val_icms_imp": pa.Column(pl.Float64, pa.Check.ge(0), nullable=False),
        "val_icms_st_sda": pa.Column(pl.Float64, pa.Check.ge(0), nullable=False),
    },
)

# ---------------------------------------------------------------------------
# ComexSchema — MDIC ComEx CSV (separador ; encoding latin-1)
# ---------------------------------------------------------------------------
ComexSchema = pa.DataFrameSchema(
    columns={
        "CO_ANO": pa.Column(pl.Int32, nullable=False),
        "CO_MES": pa.Column(
            pl.Int32,
            pa.Check.in_range(min_value=1, max_value=12),
            nullable=False,
        ),
        "SG_UF_NCM": pa.Column(pl.Utf8, nullable=False),
        "VL_FOB": pa.Column(pl.Int64, pa.Check.ge(0), nullable=False),
    },
)

# ---------------------------------------------------------------------------
# IBGESchema — resposta intermediária IBGE SIDRA Tabela 5938
#
# A API SIDRA retorna o campo "Valor" como string (ex: "124859" ou "-").
# O schema valida que a coluna existe e não contém nulos.
# ---------------------------------------------------------------------------
IBGESchema = pa.DataFrameSchema(
    columns={
        "Valor": pa.Column(pl.Utf8, nullable=False),
    },
)

# ---------------------------------------------------------------------------
# PTAXSchema — array value[] da API BCB PTAX OData convertido em DataFrame
# ---------------------------------------------------------------------------
PTAXSchema = pa.DataFrameSchema(
    columns={
        "cotacaoVenda": pa.Column(pl.Float64, pa.Check.gt(0), nullable=False),
        "dataHoraCotacao": pa.Column(pl.Utf8, nullable=False),
    },
)
