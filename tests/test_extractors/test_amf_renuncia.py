"""Testes do parser e reader da renúncia fiscal de ICMS (AMF Tabela 7 / LDO-MA).

A AMF Tabela 7 lista a renúncia de receita por TRIBUTO × MODALIDADE. No export:
- col 0: TRIBUTO (célula mesclada — só preenchida na 1ª linha do bloco; ffill);
- col 1: MODALIDADE (Crédito Presumido, Isenção, Redução de Base de Cálculo...);
- col 3+: valores por ano (col 3 = base_year, col 4 = base_year+1, ...).

O parser soma as modalidades de ICMS (IPVA fica de fora) lendo por posição.
"""

from __future__ import annotations

import sys
import types
from decimal import Decimal
from pathlib import Path

import polars as pl

from gap_tributario.extractors.amf_renuncia import AmfRenunciaParser, AmfRenunciaReader

_N_COLS = 6


def _raw(linhas: list[list]) -> pl.DataFrame:
    """DataFrame posicional ('0'..'5') como o calamine devolve (header=None)."""
    return pl.DataFrame(linhas, schema=[str(i) for i in range(_N_COLS)], orient="row")


def _fixture_planilha() -> pl.DataFrame:
    """Reproduz o layout: TRIBUTO mesclado, 3 modalidades de ICMS + uma de IPVA.

    base_year = 2022 → col 3 = 2022, col 4 = 2023, col 5 = 2024.
    """
    return _raw(
        [
            ["TRIBUTO", "MODALIDADE", "SETORES", "2022", "2023", "2024"],  # cabeçalho
            ["ICMS", "Crédito Presumido", "indústria", 1_268_007_848, 1_309_218_103, 1_351_767_692],
            [None, "Isenção", "saúde", 520_062_369, 536_964_396, 554_415_739],  # TRIBUTO mesclado
            [None, "Redução de Base de Cálculo", "cesta", 394_061_121, 406_868_108, 420_091_321],
            ["IPVA", "Isenção", "veículos", 99_000_000, 100_000_000, 101_000_000],  # fora do ICMS
        ]
    )


def test_parse_soma_modalidades_icms_e_exclui_ipva() -> None:
    """parse_planilha devolve as 3 modalidades de ICMS do ano (IPVA excluído)."""
    limpo = AmfRenunciaParser().parse_planilha(_fixture_planilha(), base_year=2022, ano=2022)

    modalidades = dict(zip(limpo["modalidade"], limpo["valor_brl"]))
    assert set(modalidades) == {"Crédito Presumido", "Isenção", "Redução de Base de Cálculo"}
    assert modalidades["Crédito Presumido"] == 1_268_007_848
    assert "Isenção" not in {m for m in modalidades if "veículos" in m}  # IPVA não entrou
    # Total ICMS 2022 = 1.268 + 520 + 394 ≈ 2.182 mi
    assert sum(modalidades.values()) == 2_182_131_338


# ---------------------------------------------------------------------------
# AmfRenunciaReader — lê o CSV limpo versionado e devolve RenunciaFiscal
# ---------------------------------------------------------------------------


def test_reader_2022_retorna_renuncia_em_milhoes() -> None:
    """ler(2022) devolve RenunciaFiscal: total ≈ 2.182 mi, 3 modalidades, vintage LDO-2022."""
    renuncia = AmfRenunciaReader().ler(2022)

    assert renuncia is not None
    assert renuncia.ano == 2022
    assert renuncia.vintage == "LDO-2022"
    assert renuncia.total == Decimal("2182.131338")
    assert set(renuncia.por_modalidade) == {
        "Crédito Presumido",
        "Isenção",
        "Redução de Base de Cálculo",
    }
    assert renuncia.por_modalidade["Crédito Presumido"] == Decimal("1268.007848")


def test_reader_ano_sem_renuncia_retorna_none() -> None:
    """Ano fora da cobertura (sem renúncia no asset) → None (degrada p/ só gap total)."""
    assert AmfRenunciaReader().ler(2030) is None


# ---------------------------------------------------------------------------
# Materialização (.xlsx → CSV) — tooling offline, calamine lazy
# ---------------------------------------------------------------------------


def test_from_xlsx_le_via_calamine_e_converte_nan(monkeypatch) -> None:
    """from_xlsx lê via pandas+calamine (mockados), converte NaN→None e faz ffill do TRIBUTO."""
    linhas = [
        ["TRIBUTO", "MODALIDADE", "SETORES", "2022", "2023", "2024"],
        ["ICMS", "Crédito Presumido", "ind", 1_268_007_848, 1.0, 2.0],
        [float("nan"), "Isenção", "saúde", 520_062_369, 3.0, 4.0],  # TRIBUTO mesclado = NaN
    ]

    class _FakeBruto:
        shape = (3, _N_COLS)

        class values:  # noqa: N801 — espelha pandas DataFrame.values.tolist()
            @staticmethod
            def tolist():
                return linhas

    class _FakeExcelFile:
        def __init__(self, *_a, **_k) -> None: ...
        def parse(self, *_a, **_k):
            return _FakeBruto()

    fake_pd = types.ModuleType("pandas")
    fake_pd.ExcelFile = _FakeExcelFile
    monkeypatch.setitem(sys.modules, "pandas", fake_pd)

    limpo = AmfRenunciaParser().from_xlsx("x.xlsx", sheet="s", base_year=2022, ano=2022)
    modalidades = dict(zip(limpo["modalidade"], limpo["valor_brl"]))
    # A linha de Isenção (TRIBUTO=NaN) só entra se o ffill recuperou "ICMS".
    assert set(modalidades) == {"Crédito Presumido", "Isenção"}


def test_materializar_csv_varre_vintages_e_grava(tmp_path: Path, monkeypatch) -> None:
    """materializar_csv varre as 8 vintages e grava (ano, vintage, modalidade, valor_brl)."""
    df = pl.DataFrame({"modalidade": ["Crédito Presumido"], "valor_brl": [1_000_000_000.0]})
    monkeypatch.setattr(
        AmfRenunciaParser, "from_xlsx", lambda self, p, *, sheet, base_year, ano: df
    )

    destino = tmp_path / "amf.csv"
    retorno = AmfRenunciaParser().materializar_csv("docs_ignorado", destino)

    assert retorno == destino
    texto = destino.read_text(encoding="utf-8")
    assert "ano,vintage,modalidade,valor_brl" in texto
    assert "2022,LDO-2022,Crédito Presumido,1000000000" in texto
    assert "2029,LDO-2026,Crédito Presumido,1000000000" in texto
