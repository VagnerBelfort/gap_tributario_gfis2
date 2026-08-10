"""Testes do extrator de importações a partir do snapshot agregado do Siscomex.

O snapshot é materializado no cluster da SEFAZ (scripts/snapshot_siscomex.py) e
traz uma linha por ano × trimestre × capítulo NCM × UF de despacho × UF do
importador. O extrator soma o CIF do importador maranhense no período.
"""

from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.siscomex import SiscomexSnapshotExtractor
from gap_tributario.models import PeriodoCalculo

_COLUNAS = ["ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_importador", "dis", "cif_brl"]


def _escrever_snapshot(path, linhas):
    """Materializa um snapshot CSV no formato exportado pelo job Spark."""
    pl.DataFrame(linhas, schema=_COLUNAS, orient="row").write_csv(path, separator=";")
    return path


def test_periodo_anual_soma_os_quatro_trimestres_do_importador_ma(tmp_path):
    """Ano completo agrega os trimestres do MA e devolve R$ milhões."""
    snapshot = _escrever_snapshot(
        tmp_path / "siscomex.csv",
        [
            (2022, 1, 27, "MA", "MA", 10, 1_000_000_000.0),
            (2022, 2, 27, "MA", "MA", 10, 2_000_000_000.0),
            (2022, 3, 31, "MA", "MA", 10, 3_000_000_000.0),
            (2022, 4, 31, "MA", "MA", 10, 4_000_000_000.0),
        ],
    )

    resultado = SiscomexSnapshotExtractor(snapshot).extract(PeriodoCalculo(ano=2022))

    assert resultado == Decimal("10000")  # R$ 10 bi = 10.000 milhões


def test_periodo_trimestral_soma_apenas_o_trimestre_pedido(tmp_path):
    """T2 traz só o segundo trimestre — o VAB trimestral depende disso."""
    snapshot = _escrever_snapshot(
        tmp_path / "siscomex.csv",
        [
            (2022, 1, 27, "MA", "MA", 10, 1_000_000_000.0),
            (2022, 2, 27, "MA", "MA", 10, 2_000_000_000.0),
            (2022, 3, 31, "MA", "MA", 10, 3_000_000_000.0),
        ],
    )

    resultado = SiscomexSnapshotExtractor(snapshot).extract(
        PeriodoCalculo(ano=2022, trimestre=2)
    )

    assert resultado == Decimal("2000")


def test_exclui_transito_de_outras_ufs_e_nao_identificado(tmp_path):
    """Carga despachada em Itaqui para outro estado não é importação do MA.

    É a correção central: o SG_UF_NCM do MDIC somava tudo que passou por
    Itaqui. Aqui só entra quem tem domicílio fiscal no MA.
    """
    snapshot = _escrever_snapshot(
        tmp_path / "siscomex.csv",
        [
            (2022, 1, 27, "MA", "MA", 10, 5_000_000_000.0),  # importador MA
            (2022, 1, 27, "MA", "TO", 10, 3_000_000_000.0),  # trânsito p/ Tocantins
            (2022, 1, 31, "MA", "NI", 10, 2_000_000_000.0),  # UF não identificada
            (2022, 1, 84, "CE", "MA", 10, 1_000_000_000.0),  # MA desembaraçando no CE
        ],
    )

    resultado = SiscomexSnapshotExtractor(snapshot).extract(PeriodoCalculo(ano=2022))

    # 5 bi do MA + 1 bi do MA despachado no CE; trânsito e NI ficam fora.
    assert resultado == Decimal("6000")


def test_incluir_nao_identificado_soma_o_balde_ni(tmp_path):
    """Sensibilidade: teto do intervalo, se todo NI for maranhense."""
    snapshot = _escrever_snapshot(
        tmp_path / "siscomex.csv",
        [
            (2022, 1, 27, "MA", "MA", 10, 5_000_000_000.0),
            (2022, 1, 31, "MA", "NI", 10, 2_000_000_000.0),
            (2022, 1, 27, "MA", "TO", 10, 3_000_000_000.0),
        ],
    )

    extractor = SiscomexSnapshotExtractor(snapshot, incluir_nao_identificado=True)

    assert extractor.extract(PeriodoCalculo(ano=2022)) == Decimal("7000")


def test_ano_sem_dados_levanta_extraction_error(tmp_path):
    """Sem dados no período, a cascata precisa cair para o MDIC.

    O Siscomex só tem cobertura confiável de 2013 em diante; devolver zero
    silenciosamente zeraria as importações e inflaria o gap.
    """
    snapshot = _escrever_snapshot(
        tmp_path / "siscomex.csv",
        [(2022, 1, 27, "MA", "MA", 10, 5_000_000_000.0)],
    )

    with pytest.raises(ExtractionError, match="2011"):
        SiscomexSnapshotExtractor(snapshot).extract(PeriodoCalculo(ano=2011))


def test_snapshot_ausente_levanta_extraction_error(tmp_path):
    """Quem roda fora da rede da SEFAZ pode não ter o snapshot — cai para o MDIC."""
    inexistente = tmp_path / "nao_existe.csv"

    with pytest.raises(ExtractionError, match="não encontrado"):
        SiscomexSnapshotExtractor(inexistente).extract(PeriodoCalculo(ano=2022))


_SNAPSHOT_REAL = Path("bases/siscomex_importacoes.csv")


@pytest.mark.skipif(not _SNAPSHOT_REAL.exists(), reason="snapshot do Siscomex ausente")
def test_golden_2022_importacoes_do_snapshot_real():
    """Golden do Siscomex: importações MA 2022 = R$ 39.704 mi (CIF).

    Medido no Oracle C3 (APL_SISCOMEX) com chave composta (NUM_DECL, SEQ_DECL),
    deduplicação pela última versão da DI, tipos 01 e nulo, situações S/N/nula.

    Era R$ 37.471 mi antes de resolver as DIs sem UF do importador pelo cadastro
    de contribuintes (`b_contribuinte.uf_icms`): 2.812 delas eram maranhenses,
    somando R$ 7,71 bi que estavam fora da conta. Sobraram 30 DIs (R$ 9,1 mi)
    sem atribuição.

    Se este valor mudar, o snapshot foi regerado com outros filtros — revisar
    antes de aceitar.
    """
    resultado = SiscomexSnapshotExtractor(_SNAPSHOT_REAL).extract(PeriodoCalculo(ano=2022))

    assert abs(resultado - Decimal("39704")) < Decimal("1")


@pytest.mark.skipif(not _SNAPSHOT_REAL.exists(), reason="snapshot do Siscomex ausente")
def test_golden_2022_trimestres_somam_o_ano():
    """Os quatro trimestres de 2022 reconstroem o total anual."""
    extractor = SiscomexSnapshotExtractor(_SNAPSHOT_REAL)

    soma_trimestres = sum(
        extractor.extract(PeriodoCalculo(ano=2022, trimestre=t)) for t in (1, 2, 3, 4)
    )

    # Tolerância de centavos: cada período é arredondado independentemente, então
    # quatro arredondamentos podem divergir do arredondamento único do ano.
    assert abs(soma_trimestres - extractor.extract(PeriodoCalculo(ano=2022))) <= Decimal("0.05")


def test_snapshot_com_colunas_faltando_levanta_extraction_error(tmp_path):
    """Snapshot corrompido/desatualizado cai para o MDIC, não estoura traceback.

    O cli.py só captura ExtractionError; qualquer exceção crua do polars sobe
    e derruba a execução inteira em vez de acionar a próxima fonte da cascata.
    """
    ruim = tmp_path / "siscomex.csv"
    pl.DataFrame({"ano": [2022], "cif_brl": [1.0]}).write_csv(ruim, separator=";")

    with pytest.raises(ExtractionError, match="inesperado|coluna|formato"):
        SiscomexSnapshotExtractor(ruim).extract(PeriodoCalculo(ano=2022))


def test_snapshot_parquet_e_lido(tmp_path):
    """O docstring promete .parquet — então .parquet tem que funcionar."""
    caminho = tmp_path / "siscomex.parquet"
    pl.DataFrame(
        [(2022, 1, 27, "MA", "MA", 10, 5_000_000_000.0)], schema=_COLUNAS, orient="row"
    ).write_parquet(caminho)

    assert SiscomexSnapshotExtractor(caminho).extract(PeriodoCalculo(ano=2022)) == Decimal("5000")
