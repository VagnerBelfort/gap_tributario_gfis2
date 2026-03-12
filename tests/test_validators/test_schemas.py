"""Testes dos validators Pandera.

Cobertura completa: happy paths, erros de nulo, valores negativos,
tipo errado e colunas ausentes para cada um dos quatro schemas.
"""

from __future__ import annotations

import polars as pl
import pytest
from pandera.errors import SchemaError

from gap_tributario.validators.schemas import (
    ArrecadacaoSchema,
    ComexSchema,
    IBGESchema,
    PTAXSchema,
)

# ---------------------------------------------------------------------------
# Helpers de DataFrames válidos
# ---------------------------------------------------------------------------


def _valid_arrecadacao() -> pl.DataFrame:
    """DataFrame válido para ArrecadacaoSchema."""
    return pl.DataFrame(
        {
            "per_aaaa": pl.Series([2022, 2022], dtype=pl.Int32),
            "per_nro_trimestre": pl.Series([1, 2], dtype=pl.Int32),
            "val_icms_normal": pl.Series([1000.0, 2000.0], dtype=pl.Float64),
            "val_icms_imp": pl.Series([100.0, 200.0], dtype=pl.Float64),
            "val_icms_st_sda": pl.Series([50.0, 75.0], dtype=pl.Float64),
        }
    )


def _valid_comex() -> pl.DataFrame:
    """DataFrame válido para ComexSchema."""
    return pl.DataFrame(
        {
            "CO_ANO": pl.Series([2022, 2022], dtype=pl.Int32),
            "CO_MES": pl.Series([1, 12], dtype=pl.Int32),
            "SG_UF_NCM": pl.Series(["MA", "MA"], dtype=pl.Utf8),
            "VL_FOB": pl.Series([1000000, 2000000], dtype=pl.Int64),
        }
    )


def _valid_ibge() -> pl.DataFrame:
    """DataFrame válido para IBGESchema."""
    return pl.DataFrame(
        {
            "Valor": pl.Series(["124859", "130000"], dtype=pl.Utf8),
        }
    )


def _valid_ptax() -> pl.DataFrame:
    """DataFrame válido para PTAXSchema."""
    return pl.DataFrame(
        {
            "cotacaoVenda": pl.Series([5.22, 5.25], dtype=pl.Float64),
            "dataHoraCotacao": pl.Series(
                ["2022-01-03 13:04:21.0", "2022-01-04 13:05:00.0"], dtype=pl.Utf8
            ),
        }
    )


# ---------------------------------------------------------------------------
# Testes de importabilidade
# ---------------------------------------------------------------------------


def test_schemas_importable():
    """Verifica que todos os schemas são importáveis."""
    assert ArrecadacaoSchema is not None
    assert ComexSchema is not None
    assert IBGESchema is not None
    assert PTAXSchema is not None


# ---------------------------------------------------------------------------
# ArrecadacaoSchema — happy path
# ---------------------------------------------------------------------------


def test_arrecadacao_happy_path():
    """DataFrame válido com todos os campos corretos passa sem erro."""
    df = _valid_arrecadacao()
    validated = ArrecadacaoSchema.validate(df)
    assert validated is not None


def test_arrecadacao_extra_columns_ignored():
    """Colunas extras no DataFrame não causam falha (strict=False padrão)."""
    df = _valid_arrecadacao().with_columns(pl.lit("extra").alias("coluna_extra"))
    validated = ArrecadacaoSchema.validate(df)
    assert validated is not None


# ---------------------------------------------------------------------------
# ArrecadacaoSchema — erros de nulo
# ---------------------------------------------------------------------------


def test_arrecadacao_null_val_icms_normal_raises():
    """Nulo em val_icms_normal deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_normal", [None, 2000.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


def test_arrecadacao_null_val_icms_imp_raises():
    """Nulo em val_icms_imp deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_imp", [None, 200.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


def test_arrecadacao_null_val_icms_st_sda_raises():
    """Nulo em val_icms_st_sda deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_st_sda", [None, 75.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


# ---------------------------------------------------------------------------
# ArrecadacaoSchema — valores negativos
# ---------------------------------------------------------------------------


def test_arrecadacao_negative_val_icms_normal_raises():
    """Valor negativo em val_icms_normal deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_normal", [-1.0, 2000.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


def test_arrecadacao_negative_val_icms_imp_raises():
    """Valor negativo em val_icms_imp deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_imp", [-0.01, 200.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


def test_arrecadacao_negative_val_icms_st_sda_raises():
    """Valor negativo em val_icms_st_sda deve levantar SchemaError."""
    df = _valid_arrecadacao().with_columns(
        pl.Series("val_icms_st_sda", [-50.0, 75.0], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


# ---------------------------------------------------------------------------
# ArrecadacaoSchema — coluna ausente
# ---------------------------------------------------------------------------


def test_arrecadacao_missing_column_raises():
    """DataFrame sem coluna obrigatória deve levantar SchemaError."""
    df = _valid_arrecadacao().drop("val_icms_st_sda")
    with pytest.raises(SchemaError):
        ArrecadacaoSchema.validate(df)


# ---------------------------------------------------------------------------
# ComexSchema — happy path
# ---------------------------------------------------------------------------


def test_comex_happy_path():
    """DataFrame válido MDIC ComEx passa sem erro."""
    df = _valid_comex()
    validated = ComexSchema.validate(df)
    assert validated is not None


# ---------------------------------------------------------------------------
# ComexSchema — erros de nulo
# ---------------------------------------------------------------------------


def test_comex_null_vl_fob_raises():
    """Nulo em VL_FOB deve levantar SchemaError."""
    df = _valid_comex().with_columns(
        pl.Series("VL_FOB", [None, 2000000], dtype=pl.Int64)
    )
    with pytest.raises(SchemaError):
        ComexSchema.validate(df)


# ---------------------------------------------------------------------------
# ComexSchema — valor negativo
# ---------------------------------------------------------------------------


def test_comex_negative_vl_fob_raises():
    """VL_FOB negativo deve levantar SchemaError."""
    df = _valid_comex().with_columns(
        pl.Series("VL_FOB", [-1, 2000000], dtype=pl.Int64)
    )
    with pytest.raises(SchemaError):
        ComexSchema.validate(df)


# ---------------------------------------------------------------------------
# ComexSchema — CO_MES fora de faixa
# ---------------------------------------------------------------------------


def test_comex_co_mes_zero_raises():
    """CO_MES = 0 (abaixo do mínimo 1) deve levantar SchemaError."""
    df = _valid_comex().with_columns(
        pl.Series("CO_MES", [0, 12], dtype=pl.Int32)
    )
    with pytest.raises(SchemaError):
        ComexSchema.validate(df)


def test_comex_co_mes_13_raises():
    """CO_MES = 13 (acima do máximo 12) deve levantar SchemaError."""
    df = _valid_comex().with_columns(
        pl.Series("CO_MES", [1, 13], dtype=pl.Int32)
    )
    with pytest.raises(SchemaError):
        ComexSchema.validate(df)


# ---------------------------------------------------------------------------
# ComexSchema — coluna ausente
# ---------------------------------------------------------------------------


def test_comex_missing_sg_uf_ncm_raises():
    """DataFrame sem coluna SG_UF_NCM deve levantar SchemaError."""
    df = _valid_comex().drop("SG_UF_NCM")
    with pytest.raises(SchemaError):
        ComexSchema.validate(df)


# ---------------------------------------------------------------------------
# IBGESchema — happy path
# ---------------------------------------------------------------------------


def test_ibge_happy_path():
    """DataFrame válido IBGE SIDRA passa sem erro."""
    df = _valid_ibge()
    validated = IBGESchema.validate(df)
    assert validated is not None


# ---------------------------------------------------------------------------
# IBGESchema — erro de nulo
# ---------------------------------------------------------------------------


def test_ibge_null_valor_raises():
    """Nulo em Valor deve levantar SchemaError."""
    df = pl.DataFrame(
        {"Valor": pl.Series(["124859", None], dtype=pl.Utf8)}
    )
    with pytest.raises(SchemaError):
        IBGESchema.validate(df)


# ---------------------------------------------------------------------------
# PTAXSchema — happy path
# ---------------------------------------------------------------------------


def test_ptax_happy_path():
    """DataFrame válido BCB PTAX passa sem erro."""
    df = _valid_ptax()
    validated = PTAXSchema.validate(df)
    assert validated is not None


# ---------------------------------------------------------------------------
# PTAXSchema — erros de nulo
# ---------------------------------------------------------------------------


def test_ptax_null_cotacao_venda_raises():
    """Nulo em cotacaoVenda deve levantar SchemaError."""
    df = _valid_ptax().with_columns(
        pl.Series("cotacaoVenda", [None, 5.25], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        PTAXSchema.validate(df)


def test_ptax_null_data_hora_cotacao_raises():
    """Nulo em dataHoraCotacao deve levantar SchemaError."""
    df = _valid_ptax().with_columns(
        pl.Series(
            "dataHoraCotacao",
            [None, "2022-01-04 13:05:00.0"],
            dtype=pl.Utf8,
        )
    )
    with pytest.raises(SchemaError):
        PTAXSchema.validate(df)


# ---------------------------------------------------------------------------
# PTAXSchema — cotacaoVenda <= 0
# ---------------------------------------------------------------------------


def test_ptax_cotacao_venda_zero_raises():
    """cotacaoVenda = 0 (não positivo) deve levantar SchemaError."""
    df = _valid_ptax().with_columns(
        pl.Series("cotacaoVenda", [0.0, 5.25], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        PTAXSchema.validate(df)


def test_ptax_cotacao_venda_negative_raises():
    """cotacaoVenda negativo deve levantar SchemaError."""
    df = _valid_ptax().with_columns(
        pl.Series("cotacaoVenda", [-1.0, 5.25], dtype=pl.Float64)
    )
    with pytest.raises(SchemaError):
        PTAXSchema.validate(df)
