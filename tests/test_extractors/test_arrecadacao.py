"""Testes do extrator de arrecadação GFIS2 Parquet.

Testa o ArrecadacaoExtractor com fixtures Parquet para garantir:
- Leitura e agregação correta das colunas ICMS
- Filtragem por período anual e trimestral
- Conversão de unidade (R$ unitário → R$ milhões)
- Tipo de retorno Decimal
- Tratamento correto de erros (path inexistente, Parquet corrompido, sem dados)
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from gap_tributario.extractors.arrecadacao import ArrecadacaoExtractor
from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

# Caminho para o fixture Parquet estático
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "parquet"
FIXTURE_PARQUET = FIXTURE_DIR / "arrecadacao_fixture.parquet"

# Valores esperados calculados com base nos dados da fixture:
# 2022 anual: val_icms_normal=7.000.000 + val_icms_imp=300.000 + val_icms_st_sda=2.500.000
#             = 9.800.000 R$ unitário = 9.8 R$ milhões
ICMS_2022_ANUAL_MILHOES = Decimal("9.8")

# T1/2022: val_icms_normal=1.000.000 + val_icms_imp=100.000 + val_icms_st_sda=500.000
#          = 1.600.000 R$ unitário = 1.6 R$ milhões
ICMS_2022_T1_MILHOES = Decimal("1.6")


# ---- Fixtures pytest ----


@pytest.fixture
def extractor(tmp_path: Path) -> ArrecadacaoExtractor:
    """Copia a fixture Parquet para tmp_path e retorna extrator configurado."""
    parquet_dir = tmp_path / "g_arrecadacao"
    parquet_dir.mkdir()
    # Copia o arquivo de fixture para o diretório temporário
    conteudo = FIXTURE_PARQUET.read_bytes()
    (parquet_dir / "arrecadacao_fixture.parquet").write_bytes(conteudo)
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


@pytest.fixture
def extractor_sem_dados_2022(tmp_path: Path) -> ArrecadacaoExtractor:
    """Cria extrator com Parquet que contém apenas dados de 2021 (sem 2022)."""
    parquet_dir = tmp_path / "g_arrecadacao_sem_2022"
    parquet_dir.mkdir()

    # Apenas dados de 2021
    df = pl.DataFrame(
        {
            "per_aaaa": [2021, 2021],
            "per_nro_trimestre": [1, 2],
            "val_icms_normal": [500_000.0, 600_000.0],
            "val_icms_imp": [50_000.0, 60_000.0],
            "val_icms_st_sda": [250_000.0, 300_000.0],
        },
        schema={
            "per_aaaa": pl.Int32,
            "per_nro_trimestre": pl.Int32,
            "val_icms_normal": pl.Float64,
            "val_icms_imp": pl.Float64,
            "val_icms_st_sda": pl.Float64,
        },
    )
    df.write_parquet(parquet_dir / "dados.parquet")
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


@pytest.fixture
def extractor_valores_zero(tmp_path: Path) -> ArrecadacaoExtractor:
    """Cria extrator com Parquet onde todos os valores ICMS são zero."""
    parquet_dir = tmp_path / "g_arrecadacao_zeros"
    parquet_dir.mkdir()

    df = pl.DataFrame(
        {
            "per_aaaa": [2022],
            "per_nro_trimestre": [1],
            "val_icms_normal": [0.0],
            "val_icms_imp": [0.0],
            "val_icms_st_sda": [0.0],
        },
        schema={
            "per_aaaa": pl.Int32,
            "per_nro_trimestre": pl.Int32,
            "val_icms_normal": pl.Float64,
            "val_icms_imp": pl.Float64,
            "val_icms_st_sda": pl.Float64,
        },
    )
    df.write_parquet(parquet_dir / "dados.parquet")
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


# ---- Testes básicos de importabilidade e instanciação ----


def test_arrecadacao_extractor_importable():
    """Verifica que o extrator é importável."""
    assert ArrecadacaoExtractor is not None


def test_arrecadacao_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado."""
    extractor = ArrecadacaoExtractor(parquet_base_path="./test_path")
    assert extractor.parquet_base_path == "./test_path"


# ---- Testes de happy path — período anual ----


def test_extract_periodo_anual_retorna_valor_correto(extractor):
    """Fixture com dados 2022 (T1-T4) → soma anual = 9.8 M.

    Dados da fixture:
    - val_icms_normal: 7.000.000 R$ = 7.0 M
    - val_icms_imp:    300.000 R$   = 0.3 M
    - val_icms_st_sda: 2.500.000 R$ = 2.5 M
    - Total 2022:      9.800.000 R$ = 9.8 M
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    assert resultado == ICMS_2022_ANUAL_MILHOES


def test_extract_retorna_decimal_nao_float(extractor):
    """Verifica que o tipo de retorno é Decimal, não float."""
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert not isinstance(resultado, float)


def test_extract_valor_em_milhoes_nao_reais(extractor):
    """Verifica que o resultado está em milhões, não em R$ unitário.

    Total 2022 = 9.800.000 R$ → resultado deve ser ~9.8, não ~9.800.000.
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    # Deve ser ~9.8 M, não ~9.800.000
    assert resultado < Decimal("1000"), f"Valor muito alto ({resultado}): provavelmente em R$ unitário"
    assert resultado > Decimal("1"), f"Valor muito baixo ({resultado}): verificar conversão"
    assert resultado == Decimal("9.8")


# ---- Testes de happy path — período trimestral ----


def test_extract_periodo_trimestral_t1_retorna_valor_correto(extractor):
    """Fixture com dados T1/2022 → soma T1 = 1.6 M.

    Dados da fixture para T1:
    - val_icms_normal: 1.000.000 R$ = 1.0 M
    - val_icms_imp:    100.000 R$   = 0.1 M
    - val_icms_st_sda: 500.000 R$   = 0.5 M
    - Total T1/2022:   1.600.000 R$ = 1.6 M
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))

    assert resultado == ICMS_2022_T1_MILHOES


def test_extract_trimestral_retorna_apenas_dados_do_trimestre(extractor):
    """Verifica que a filtragem trimestral não inclui outros trimestres.

    T1/2022 (1.6 M) < Total anual/4 confirma que está filtrando corretamente.
    """
    resultado_t1 = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))
    resultado_anual = extractor.extract(PeriodoCalculo(ano=2022))

    # T1 deve ser menor que o total anual
    assert resultado_t1 < resultado_anual

    # T1 deve ser exatamente o valor do trimestre 1, não 1/4 do anual
    assert resultado_t1 == ICMS_2022_T1_MILHOES
    assert resultado_anual == ICMS_2022_ANUAL_MILHOES


def test_extract_filtragem_exclui_dados_de_outros_anos(extractor):
    """Verifica que dados de 2021 não interferem no resultado de 2022."""
    resultado_2022 = extractor.extract(PeriodoCalculo(ano=2022))

    # O fixture contém dados de 2021 também; se não filtrar corretamente,
    # o resultado seria maior que 9.8 M
    assert resultado_2022 == ICMS_2022_ANUAL_MILHOES


# ---- Testes de valores zero ----


def test_extract_valores_icms_zero_retorna_decimal_zero(extractor_valores_zero):
    """Fixture com todos os valores ICMS = 0 → deve retornar Decimal('0'), não erro."""
    resultado = extractor_valores_zero.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado == Decimal("0")


# ---- Testes de erro — path inexistente ----


def test_extract_path_inexistente_levanta_extraction_error():
    """ArrecadacaoExtractor com path que não existe → deve levantar ExtractionError."""
    extractor = ArrecadacaoExtractor(parquet_base_path="/caminho/que/nao/existe/parquet")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "não encontrado" in mensagem or "não existe" in mensagem or "not found" in mensagem.lower()
    assert "/caminho/que/nao/existe/parquet" in mensagem


def test_extract_path_inexistente_menciona_configuracao():
    """Mensagem de erro deve orientar o usuário a verificar aliquotas.yaml."""
    extractor = ArrecadacaoExtractor(parquet_base_path="/path/invalido")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    # Mensagem deve orientar o usuário a verificar a configuração
    assert "aliquotas.yaml" in str(exc_info.value) or "parquet_base_path" in str(exc_info.value)


# ---- Testes de erro — sem dados para o período ----


def test_extract_sem_dados_para_periodo_levanta_extraction_error(extractor_sem_dados_2022):
    """Parquet com apenas dados de 2021 e consulta de 2022 → ExtractionError."""
    with pytest.raises(ExtractionError) as exc_info:
        extractor_sem_dados_2022.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "2022" in mensagem


def test_extract_sem_dados_para_trimestre_levanta_extraction_error(extractor_sem_dados_2022):
    """Parquet sem dados de 2022 e consulta T1/2022 → ExtractionError."""
    with pytest.raises(ExtractionError) as exc_info:
        extractor_sem_dados_2022.extract(PeriodoCalculo(ano=2022, trimestre=1))

    assert "2022" in str(exc_info.value)


# ---- Testes de Parquet corrompido ----


def test_extract_parquet_corrompido_levanta_extraction_error(tmp_path):
    """Arquivo com extensão .parquet mas conteúdo inválido → ExtractionError."""
    parquet_dir = tmp_path / "g_arrecadacao_corrompido"
    parquet_dir.mkdir()

    # Cria arquivo com conteúdo inválido (não é um Parquet válido)
    arquivo_invalido = parquet_dir / "corrompido.parquet"
    arquivo_invalido.write_bytes(b"conteudo invalido que nao eh parquet")

    extractor = ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "Erro ao ler" in mensagem or "corrompido" in mensagem or "parquet" in mensagem.lower()


# ---- Testes de conversão de unidade ----


def test_extract_conversao_unidade_reais_para_milhoes(extractor):
    """Verificação explícita da conversão: soma bruta é 9.800.000 → resultado deve ser 9.8.

    A fixture tem:
    - T1: normal=1M + imp=100k + st=500k = 1.6M
    - T2: normal=2M + imp=200k + st=1M   = 3.2M
    - T3: normal=3M + imp=0   + st=500k  = 3.5M
    - T4: normal=1M + imp=0   + st=500k  = 1.5M
    Total bruto: 9.800.000 R$ → 9.8 R$ milhões
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    # Verificação explícita: 9800000 / 1000000 = 9.8
    assert resultado == Decimal("9800000") / Decimal("1000000")
    assert resultado == Decimal("9.8")
