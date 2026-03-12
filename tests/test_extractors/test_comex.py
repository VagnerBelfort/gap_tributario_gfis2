"""Testes do extrator MDIC ComEx.

Testa o ComexExtractor com fixtures CSV para garantir:
- Leitura e agregação correta das colunas VL_FOB
- Filtragem por UF=MA (exclui outras UFs)
- Filtragem por período anual e trimestral
- Conversão USD → R$ milhões usando PTAX
- Tipo de retorno Tuple[Decimal, Decimal]
- Tratamento correto de erros (path inexistente, CSV inválido, sem dados)
"""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.comex import ComexExtractor
from gap_tributario.models import PeriodoCalculo

# Caminho para os fixtures CSV estáticos
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "csv"

# Valores esperados com base nos dados dos fixtures
# PTAX mock = 5.0 R$/USD (para cálculos simples)
PTAX_MOCK = Decimal("5.0")

# EXP 2022 MA:
# T1 (Jan-Mar): 100k + 100k + 100k = 300.000 USD
# T2 (Abr-Jun): 150k + 150k + 150k = 450.000 USD
# T3 (Jul-Set): 100k + 100k + 100k = 300.000 USD
# T4 (Out-Dez): 75k  + 75k  + 100k = 250.000 USD
# Total: 1.300.000 USD × 5.0 / 1.000.000 = 6.5 M R$
EXP_ANUAL_2022_MILHOES = Decimal("6.5")

# EXP T1 2022 MA: 300.000 USD × 5.0 / 1.000.000 = 1.5 M R$
EXP_T1_2022_MILHOES = Decimal("1.5")

# IMP 2022 MA:
# T1 (Jan-Mar): 50k + 50k + 50k = 150.000 USD
# T2 (Abr-Jun): 75k + 75k + 75k = 225.000 USD
# T3 (Jul-Set): 50k + 50k + 50k = 150.000 USD
# T4 (Out-Dez): 50k + 50k + 75k = 175.000 USD
# Total: 700.000 USD × 5.0 / 1.000.000 = 3.5 M R$
IMP_ANUAL_2022_MILHOES = Decimal("3.5")

# IMP T1 2022 MA: 150.000 USD × 5.0 / 1.000.000 = 0.75 M R$
IMP_T1_2022_MILHOES = Decimal("0.75")


# ---- Fixtures pytest ----


@pytest.fixture
def extractor(tmp_path: Path) -> ComexExtractor:
    """Copia as fixtures CSV para tmp_path e retorna extrator configurado."""
    csv_dir = tmp_path / "mdic_comex"
    csv_dir.mkdir()
    shutil.copy(FIXTURE_DIR / "EXP_2022.csv", csv_dir / "EXP_2022.csv")
    shutil.copy(FIXTURE_DIR / "IMP_2022.csv", csv_dir / "IMP_2022.csv")
    return ComexExtractor(mdic_base_path=str(csv_dir))


@pytest.fixture
def extractor_sem_dados_ma(tmp_path: Path) -> ComexExtractor:
    """Cria extrator com CSVs que não contêm dados do Maranhão."""
    csv_dir = tmp_path / "mdic_comex_sem_ma"
    csv_dir.mkdir()

    # CSVs com apenas dados de SP e RJ (sem MA)
    conteudo_exp = (
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n"
        "2022;1;SP;500000\n"
        "2022;2;RJ;300000\n"
    )
    conteudo_imp = (
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n"
        "2022;1;SP;200000\n"
        "2022;3;RJ;100000\n"
    )
    (csv_dir / "EXP_2022.csv").write_text(conteudo_exp, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo_imp, encoding="latin-1")
    return ComexExtractor(mdic_base_path=str(csv_dir))


@pytest.fixture
def extractor_somente_2021(tmp_path: Path) -> ComexExtractor:
    """Cria extrator com CSVs que contêm apenas dados de 2021."""
    csv_dir = tmp_path / "mdic_comex_2021"
    csv_dir.mkdir()

    # Sem CO_ANO 2022
    conteudo_exp = (
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n"
        "2021;1;MA;200000\n"
        "2021;6;MA;150000\n"
    )
    conteudo_imp = (
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n"
        "2021;1;MA;80000\n"
        "2021;12;MA;60000\n"
    )
    (csv_dir / "EXP_2022.csv").write_text(conteudo_exp, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo_imp, encoding="latin-1")
    return ComexExtractor(mdic_base_path=str(csv_dir))


# ---- Testes básicos de importabilidade e instanciação ----


def test_comex_extractor_importable():
    """Verifica que o extrator é importável."""
    assert ComexExtractor is not None


def test_comex_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado."""
    extractor = ComexExtractor(mdic_base_path="./test_path")
    assert extractor.mdic_base_path == "./test_path"


# ---- Testes de happy path — período anual ----


def test_extract_periodo_anual_retorna_exportacoes_corretas(extractor):
    """Fixture com dados EXP 2022 MA → soma anual = 6.5 M R$."""
    exp_brl, _ = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert exp_brl == EXP_ANUAL_2022_MILHOES


def test_extract_periodo_anual_retorna_importacoes_corretas(extractor):
    """Fixture com dados IMP 2022 MA → soma anual = 3.5 M R$."""
    _, imp_brl = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert imp_brl == IMP_ANUAL_2022_MILHOES


def test_extract_retorna_tuple_de_dois_decimals(extractor):
    """Verifica que o retorno é uma tupla com dois Decimals."""
    resultado = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert isinstance(resultado, tuple)
    assert len(resultado) == 2
    exp_brl, imp_brl = resultado
    assert isinstance(exp_brl, Decimal)
    assert isinstance(imp_brl, Decimal)


def test_extract_valores_em_milhoes_nao_reais(extractor):
    """Verifica que os resultados estão em milhões, não em R$ unitário."""
    exp_brl, imp_brl = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    # Devem ser em torno de 6.5 e 3.5, não 6.500.000 e 3.500.000
    assert exp_brl < Decimal("1000"), f"EXP muito alto ({exp_brl}): provavelmente em R$ unitário"
    assert imp_brl < Decimal("1000"), f"IMP muito alto ({imp_brl}): provavelmente em R$ unitário"
    assert exp_brl > Decimal("0"), "EXP não pode ser zero"
    assert imp_brl > Decimal("0"), "IMP não pode ser zero"


# ---- Testes de happy path — período trimestral ----


def test_extract_periodo_trimestral_t1_exportacoes_corretas(extractor):
    """Fixture com dados T1/2022 MA → EXP T1 = 1.5 M R$."""
    exp_brl, _ = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1), PTAX_MOCK)

    assert exp_brl == EXP_T1_2022_MILHOES


def test_extract_periodo_trimestral_t1_importacoes_corretas(extractor):
    """Fixture com dados T1/2022 MA → IMP T1 = 0.75 M R$."""
    _, imp_brl = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1), PTAX_MOCK)

    assert imp_brl == IMP_T1_2022_MILHOES


def test_extract_trimestral_menor_que_anual(extractor):
    """T1 deve ser menor que o total anual."""
    exp_brl_t1, imp_brl_t1 = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1), PTAX_MOCK)
    exp_brl_anual, imp_brl_anual = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert exp_brl_t1 < exp_brl_anual
    assert imp_brl_t1 < imp_brl_anual


def test_extract_filtragem_trimestral_exclui_outros_trimestres(extractor):
    """T1 e T2 devem ser diferentes (dados diferentes nos fixtures)."""
    exp_t1, imp_t1 = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1), PTAX_MOCK)
    exp_t2, imp_t2 = extractor.extract(PeriodoCalculo(ano=2022, trimestre=2), PTAX_MOCK)

    # T2 tem mais exportações que T1 nos fixtures (450k vs 300k USD)
    assert exp_t2 > exp_t1


# ---- Testes de filtragem de UF ----


def test_extract_filtra_apenas_uf_maranhao(extractor):
    """Fixture com dados de SP e RJ → não devem impactar resultado de MA."""
    exp_brl, imp_brl = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    # Resultado deve ser apenas dos dados de MA (não incluindo SP e RJ dos fixtures)
    assert exp_brl == EXP_ANUAL_2022_MILHOES
    assert imp_brl == IMP_ANUAL_2022_MILHOES


def test_extract_sem_dados_ma_retorna_zeros(extractor_sem_dados_ma):
    """CSV sem dados do MA → deve retornar (0.0, 0.0) não erro."""
    exp_brl, imp_brl = extractor_sem_dados_ma.extract(
        PeriodoCalculo(ano=2022), PTAX_MOCK
    )

    assert exp_brl == Decimal("0")
    assert imp_brl == Decimal("0")


# ---- Testes de filtragem de ano ----


def test_extract_filtra_apenas_ano_correto(extractor):
    """Fixture com dados de 2021 → não devem impactar resultado de 2022."""
    exp_brl, imp_brl = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    # Resultado não deve incluir dados de 2021 presentes nos fixtures
    assert exp_brl == EXP_ANUAL_2022_MILHOES
    assert imp_brl == IMP_ANUAL_2022_MILHOES


def test_extract_sem_dados_para_ano_retorna_zeros(extractor_somente_2021):
    """CSV com apenas dados de 2021, consulta de 2022 → deve retornar zeros."""
    exp_brl, imp_brl = extractor_somente_2021.extract(
        PeriodoCalculo(ano=2022), PTAX_MOCK
    )

    assert exp_brl == Decimal("0")
    assert imp_brl == Decimal("0")


# ---- Testes de conversão PTAX ----


def test_extract_conversao_ptax_dobrada(extractor):
    """Com PTAX 2× maior, resultado deve ser 2× maior."""
    ptax_base = Decimal("5.0")
    ptax_dobrada = Decimal("10.0")

    exp_base, imp_base = extractor.extract(PeriodoCalculo(ano=2022), ptax_base)
    exp_dobrada, imp_dobrada = extractor.extract(PeriodoCalculo(ano=2022), ptax_dobrada)

    assert exp_dobrada == exp_base * 2
    assert imp_dobrada == imp_base * 2


def test_extract_conversao_ptax_correta_exportacoes(extractor):
    """Verificação explícita: 1.300.000 USD × 5.0 / 1.000.000 = 6.5 M R$."""
    exp_brl, _ = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    # 1.300.000 USD × 5.0 = 6.500.000 R$ = 6.5 M R$
    assert exp_brl == Decimal("1300000") * PTAX_MOCK / Decimal("1000000")


def test_extract_conversao_ptax_correta_importacoes(extractor):
    """Verificação explícita: 700.000 USD × 5.0 / 1.000.000 = 3.5 M R$."""
    _, imp_brl = extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    # 700.000 USD × 5.0 = 3.500.000 R$ = 3.5 M R$
    assert imp_brl == Decimal("700000") * PTAX_MOCK / Decimal("1000000")


# ---- Testes de erro — path inexistente ----


def test_extract_path_inexistente_levanta_extraction_error():
    """ComexExtractor com path que não existe → deve levantar ExtractionError."""
    extractor = ComexExtractor(mdic_base_path="/caminho/que/nao/existe/mdic")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    mensagem = str(exc_info.value)
    assert "não encontrado" in mensagem or "not found" in mensagem.lower()
    assert "/caminho/que/nao/existe/mdic" in mensagem


def test_extract_path_inexistente_menciona_configuracao():
    """Mensagem de erro deve orientar o usuário a verificar aliquotas.yaml."""
    extractor = ComexExtractor(mdic_base_path="/path/invalido")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert "mdic_base_path" in str(exc_info.value) or "aliquotas.yaml" in str(exc_info.value)


# ---- Testes de erro — CSV não encontrado ----


def test_extract_sem_csv_exp_levanta_extraction_error(tmp_path):
    """Diretório sem CSVs de exportação → deve levantar ExtractionError."""
    csv_dir = tmp_path / "mdic_sem_exp"
    csv_dir.mkdir()
    # Cria apenas o CSV de importação
    (csv_dir / "IMP_2022.csv").write_text(
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;50000\n",
        encoding="latin-1",
    )

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    mensagem = str(exc_info.value)
    assert "exportações" in mensagem or "EXP" in mensagem
    assert "2022" in mensagem


def test_extract_sem_csv_imp_levanta_extraction_error(tmp_path):
    """Diretório sem CSVs de importação → deve levantar ExtractionError."""
    csv_dir = tmp_path / "mdic_sem_imp"
    csv_dir.mkdir()
    # Cria apenas o CSV de exportação
    (csv_dir / "EXP_2022.csv").write_text(
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;100000\n",
        encoding="latin-1",
    )

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    mensagem = str(exc_info.value)
    assert "importações" in mensagem or "IMP" in mensagem
    assert "2022" in mensagem


def test_extract_mensagem_erro_menciona_balanca(tmp_path):
    """Mensagem de erro de CSV ausente deve mencionar balanca.economia.gov.br."""
    csv_dir = tmp_path / "mdic_vazio"
    csv_dir.mkdir()

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert "balanca.economia.gov.br" in str(exc_info.value)


# ---- Testes de erro — CSV com colunas inválidas ----


def test_extract_csv_sem_coluna_sg_uf_levanta_extraction_error(tmp_path):
    """CSV sem coluna SG_UF_NCM → deve levantar ExtractionError com mensagem clara."""
    csv_dir = tmp_path / "mdic_sem_uf"
    csv_dir.mkdir()

    # CSV sem a coluna SG_UF_NCM
    conteudo = "CO_ANO;CO_MES;VL_FOB\n2022;1;100000\n"
    (csv_dir / "EXP_2022.csv").write_text(conteudo, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo, encoding="latin-1")

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert "SG_UF_NCM" in str(exc_info.value)


def test_extract_csv_sem_coluna_vl_fob_levanta_extraction_error(tmp_path):
    """CSV sem coluna VL_FOB → deve levantar ExtractionError."""
    csv_dir = tmp_path / "mdic_sem_fob"
    csv_dir.mkdir()

    # CSV sem a coluna VL_FOB
    conteudo = "CO_ANO;CO_MES;SG_UF_NCM;QT_ESTAT\n2022;1;MA;100\n"
    (csv_dir / "EXP_2022.csv").write_text(conteudo, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo, encoding="latin-1")

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    assert "VL_FOB" in str(exc_info.value)


# ---- Testes de múltiplos arquivos CSV ----


def test_extract_erro_polars_levanta_extraction_error(tmp_path, monkeypatch):
    """Erro de leitura do Polars (ex: IOError) → deve levantar ExtractionError."""
    csv_dir = tmp_path / "mdic_erro_polars"
    csv_dir.mkdir()
    (csv_dir / "EXP_2022.csv").write_text(
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;100000\n", encoding="latin-1"
    )
    (csv_dir / "IMP_2022.csv").write_text(
        "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;50000\n", encoding="latin-1"
    )

    import gap_tributario.extractors.comex as comex_module

    def mock_read_csv(*args, **kwargs):
        raise RuntimeError("Erro simulado de IO ao ler CSV")

    monkeypatch.setattr(comex_module.pl, "read_csv", mock_read_csv)

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022), PTAX_MOCK)

    mensagem = str(exc_info.value)
    assert "Erro ao ler" in mensagem


def test_extract_trimestral_sem_coluna_co_mes_levanta_extraction_error(tmp_path):
    """CSV sem coluna CO_MES em consulta trimestral → deve levantar ExtractionError."""
    csv_dir = tmp_path / "mdic_sem_mes"
    csv_dir.mkdir()

    # CSV sem a coluna CO_MES (mas com CO_ANO e SG_UF_NCM)
    conteudo = "CO_ANO;SG_UF_NCM;VL_FOB\n2022;MA;100000\n"
    (csv_dir / "EXP_2022.csv").write_text(conteudo, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo, encoding="latin-1")

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022, trimestre=1), PTAX_MOCK)

    assert "CO_MES" in str(exc_info.value)


def test_extract_combina_multiplos_csvs(tmp_path):
    """Quando houver múltiplos CSVs (EXP_2022.csv, EXP_2022_NCM.csv), deve combinar."""
    csv_dir = tmp_path / "mdic_multiplos"
    csv_dir.mkdir()

    # Dois arquivos de exportação para o mesmo ano
    conteudo1 = "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;100000\n2022;2;MA;100000\n"
    conteudo2 = "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;3;MA;100000\n2022;4;MA;100000\n"
    conteudo_imp = "CO_ANO;CO_MES;SG_UF_NCM;VL_FOB\n2022;1;MA;50000\n"

    (csv_dir / "EXP_2022.csv").write_text(conteudo1, encoding="latin-1")
    (csv_dir / "EXP_2022_NCM.csv").write_text(conteudo2, encoding="latin-1")
    (csv_dir / "IMP_2022.csv").write_text(conteudo_imp, encoding="latin-1")

    extractor = ComexExtractor(mdic_base_path=str(csv_dir))
    exp_brl, _ = extractor.extract(PeriodoCalculo(ano=2022), Decimal("1.0"))

    # 4 registros × 100.000 USD × 1.0 PTAX / 1.000.000 = 0.4 M R$
    assert exp_brl == Decimal("0.4")
