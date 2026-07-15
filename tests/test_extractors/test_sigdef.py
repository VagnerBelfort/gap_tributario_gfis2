"""Testes do parser e extrator SIGDEF (ICMS arrecadado por setor).

O export SIGDEF (`docs/20260204_por-setor.xls`) tem um desalinhamento de
colunas: o ICMS total real está na coluna de **posição 21** (rotulada
erroneamente `va_icms_outras` no cabeçalho); a coluna rotulada `va_icms_total`
(posição 22) está errada. Por isso o `SigdefParser` lê por POSIÇÃO, não por
rótulo.

Layout posicional (calamine, header=None):
- 0: id_arrecadacao   (linha 0 = agregado; linha 1 = cabeçalho)
- 1: id_uf
- 3: ANO2  (ano)
- 4: MÊS   (mês)
- 21: ICMS total REAL (rotulado va_icms_outras)
- 22: va_icms_total (rótulo enganoso — NÃO usar)
"""

from __future__ import annotations

import sys
import types
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.sigdef import SigdefIcmsExtractor, SigdefParser
from gap_tributario.models import PeriodoCalculo

# Número de colunas do export real (0..22 cobrem o que nos interessa).
_N_COLS = 23


def _linha(uf: str, ano, mes, icms_pos21, icms_pos22, *, preenche=0) -> list:
    """Monta uma linha posicional com ICMS real em pos.21 e o rótulo errado em pos.22."""
    row = [preenche] * _N_COLS
    row[1] = uf
    row[3] = ano
    row[4] = mes
    row[21] = icms_pos21  # ICMS total REAL
    row[22] = icms_pos22  # va_icms_total (errado)
    return row


def _raw(linhas: list[list]) -> pl.DataFrame:
    """Constrói um DataFrame posicional (colunas '0'..'22') como o calamine devolve."""
    return pl.DataFrame(linhas, schema=[str(i) for i in range(_N_COLS)], orient="row")


def test_le_icms_total_da_posicao_21_e_nao_do_rotulo() -> None:
    """O ICMS total vem da posição 21 (não da coluna rotulada va_icms_total na 22)."""
    raw = _raw([_linha("MA", 2021, 1, icms_pos21=900_000_000, icms_pos22=123)])
    limpo = SigdefParser().parse(raw, ano_referencia=2021, sanidade=False)

    linha = limpo.filter((pl.col("uf") == "MA") & (pl.col("ano") == 2021))
    assert linha["icms_total"].item() == 900_000_000


def test_descarta_linhas_de_cabecalho_e_agregado() -> None:
    """A linha de cabeçalho (rótulos) e a de agregado (uf vazia) são descartadas."""
    cabecalho = ["id_arrecadacao", "id_uf", "co_periodo", "ANO2", "MÊS"] + [
        f"col{i}" for i in range(5, _N_COLS)
    ]
    agregado = _linha(None, None, None, icms_pos21=999, icms_pos22=0)  # uf/ano nulos
    dado = _linha("MA", 2021, 1, icms_pos21=900_000_000, icms_pos22=123)

    limpo = SigdefParser().parse(_raw([cabecalho, agregado, dado]), ano_referencia=2021, sanidade=False)

    assert limpo.height == 1
    assert limpo["uf"].item() == "MA"


def test_sanidade_nacional_passa_para_2022_realista() -> None:
    """Soma nacional 2022 na ordem de centenas de bilhões de R$ passa na sanidade."""
    raw = _raw(
        [
            _linha("SP", 2022, 1, icms_pos21=680_000_000_000, icms_pos22=0),
            _linha("MA", 2022, 1, icms_pos21=11_494_600_000, icms_pos22=0),
        ]
    )
    limpo = SigdefParser().parse(raw, ano_referencia=2022)  # sanidade=True (default)
    assert limpo.filter(pl.col("uf") == "MA")["icms_total"].item() == 11_494_600_000


def test_sanidade_nacional_falha_para_coluna_errada() -> None:
    """Se o ICMS lido for pequeno demais (offset errado), a sanidade levanta ValueError."""
    # Simula leitura da coluna errada: total nacional 2022 implausível (65 bi).
    raw = _raw([_linha("BR", 2022, 1, icms_pos21=65_000_000_000, icms_pos22=0)])
    with pytest.raises(ValueError, match="sanidade"):
        SigdefParser().parse(raw, ano_referencia=2022)


def test_sanidade_pulada_quando_ano_referencia_ausente() -> None:
    """Se o ano de referência não está nos dados, a sanidade é pulada (não levanta)."""
    raw = _raw([_linha("MA", 2021, 1, icms_pos21=900_000_000, icms_pos22=0)])
    limpo = SigdefParser().parse(raw, ano_referencia=2022)  # 2022 ausente → pula
    assert limpo.height == 1


# ---------------------------------------------------------------------------
# Materialização (.xls → parquet limpo) — tooling offline, calamine lazy
# ---------------------------------------------------------------------------


def test_from_xls_le_via_calamine_e_chama_parse(monkeypatch) -> None:
    """from_xls lê o .xls (pandas+calamine, mockados) e devolve o df limpo via parse."""
    # Valor grande o bastante para passar a sanidade nacional de 2021.
    linhas = [_linha("MA", 2021, 1, icms_pos21=500_000_000_000, icms_pos22=0)]

    class _FakeBruto:
        shape = (1, _N_COLS)

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

    limpo = SigdefParser().from_xls("ignorado.xls", ano_referencia=2021)
    assert limpo["icms_total"].item() == 500_000_000_000


def test_materializar_parquet_recorta_uf_e_grava(tmp_path: Path, monkeypatch) -> None:
    """materializar_parquet recorta a UF de interesse e grava o parquet ordenado."""
    df_limpo = pl.DataFrame(
        {
            "uf": ["MA", "SP", "MA"],
            "ano": [2022, 2022, 2021],
            "mes": [2, 1, 1],
            "icms_total": [200.0, 999.0, 100.0],
        }
    )
    monkeypatch.setattr(SigdefParser, "from_xls", lambda self, p, ano_referencia=2022: df_limpo)

    destino = tmp_path / "out.parquet"
    retorno = SigdefParser().materializar_parquet("ignorado.xls", destino, uf="MA")

    assert retorno == destino
    gravado = pl.read_parquet(destino)
    assert gravado["uf"].unique().to_list() == ["MA"]
    assert gravado["ano"].to_list() == [2021, 2022]  # ordenado por (ano, mes)


# ---------------------------------------------------------------------------
# SigdefIcmsExtractor — lê o parquet limpo e agrega mês→período
# ---------------------------------------------------------------------------


def _parquet_limpo(tmp_path: Path) -> Path:
    """Cria um parquet limpo de teste: MA + outra UF, 12 meses de 2022/2023."""
    linhas = []
    for ano in (2022, 2023):
        for mes in range(1, 13):
            linhas.append({"uf": "MA", "ano": ano, "mes": mes, "icms_total": 100_000_000.0 * mes})
            linhas.append({"uf": "SP", "ano": ano, "mes": mes, "icms_total": 9_000_000_000.0})
    caminho = tmp_path / "sigdef_icms.parquet"
    pl.DataFrame(linhas).write_parquet(caminho)
    return caminho


def test_extract_anual_soma_doze_meses_de_ma(tmp_path: Path) -> None:
    """Período anual soma os 12 meses do MA e converte para R$ milhões."""
    caminho = _parquet_limpo(tmp_path)
    # soma 1..12 = 78 → 78 × 100_000_000 = 7_800_000_000 R$ = 7.800 milhões
    icms = SigdefIcmsExtractor(data_path=caminho).extract(PeriodoCalculo(ano=2022))
    assert icms == Decimal("7800")


def test_extract_trimestral_soma_apenas_meses_do_trimestre(tmp_path: Path) -> None:
    """Período trimestral soma só os meses do trimestre (T1 = meses 1-3)."""
    caminho = _parquet_limpo(tmp_path)
    # T1 = meses 1+2+3 = 6 → 6 × 100_000_000 = 600_000_000 R$ = 600 milhões
    icms = SigdefIcmsExtractor(data_path=caminho).extract(PeriodoCalculo(ano=2022, trimestre=1))
    assert icms == Decimal("600")


def test_ano_fora_de_cobertura_levanta_extraction_error(tmp_path: Path) -> None:
    """Ano sem dados no parquet sinaliza fallback via ExtractionError."""
    caminho = _parquet_limpo(tmp_path)
    with pytest.raises(ExtractionError):
        SigdefIcmsExtractor(data_path=caminho).extract(PeriodoCalculo(ano=2024))


def test_parquet_ausente_levanta_extraction_error(tmp_path: Path) -> None:
    """Parquet inexistente sinaliza fallback para o GFIS2 via ExtractionError."""
    with pytest.raises(ExtractionError):
        SigdefIcmsExtractor(data_path=tmp_path / "nao_existe.parquet").extract(
            PeriodoCalculo(ano=2022)
        )


def test_parquet_corrompido_levanta_extraction_error(tmp_path: Path) -> None:
    """Arquivo existente mas não-parquet → PolarsError convertido em ExtractionError."""
    corrompido = tmp_path / "corrompido.parquet"
    corrompido.write_text("isto não é um parquet válido", encoding="utf-8")
    with pytest.raises(ExtractionError):
        SigdefIcmsExtractor(data_path=corrompido).extract(PeriodoCalculo(ano=2022))


def test_proveniencia_descreve_fonte_sigdef() -> None:
    """proveniencia() carrega a origem SIGDEF, a variável ICMS e a data injetada."""
    from gap_tributario.models import Proveniencia

    prov = SigdefIcmsExtractor().proveniencia("2026-06-08")

    assert isinstance(prov, Proveniencia)
    assert prov.variavel == "ICMS Arrecadado"
    assert "SIGDEF" in prov.origem
    assert prov.data_extracao == "2026-06-08"
