"""Testes do extrator IBGE SIDRA.

Testa o IBGEExtractor com mocks de sidrapy.get_table() para garantir:
- Parsing correto do VAB em milhões R$
- Conversão de unidade (mil reais → milhões R$)
- Mecanismo de retry com backoff exponencial
- Tratamento correto de erros
- Divisão por 4 para períodos trimestrais
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import call, patch

import pandas as pd
import pytest
import requests

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.ibge import IBGEExtractor, _FATOR_VAB_PIB
from gap_tributario.models import PeriodoCalculo

# PIB MA 2022 em Mil Reais (variável 37, tabela 5938)
_PIB_MIL_REAIS = "139789146"
# VAB esperado = PIB (em milhões) × fator
_VAB_ESPERADO = Decimal(_PIB_MIL_REAIS) / Decimal("1000") * _FATOR_VAB_PIB


@pytest.fixture
def ibge_extractor():
    """Extrator IBGE com timeout curto para testes."""
    return IBGEExtractor(timeout=5, max_retries=3)


@pytest.fixture
def ibge_sidra_5938_2022_response():
    """DataFrame simulando resposta real de sidrapy.get_table() para MA 2022.

    sidrapy retorna um DataFrame pandas com:
    - Linha 0: cabeçalhos de metadados
    - Linha 1+: dados reais
    - Coluna V: valor numérico (PIB em Mil Reais)
    """
    return pd.DataFrame({
        "NC": ["Nível Territorial (Código)", "3"],
        "NN": ["Nível Territorial", "Unidade da Federação"],
        "MC": ["Unidade de Medida (Código)", "40"],
        "MN": ["Unidade de Medida", "Mil Reais"],
        "V": ["Valor", _PIB_MIL_REAIS],
        "D1C": ["Unidade da Federação (Código)", "21"],
        "D1N": ["Unidade da Federação", "Maranhão"],
        "D2C": ["Ano (Código)", "2022"],
        "D2N": ["Ano", "2022"],
        "D3C": ["Variável (Código)", "37"],
        "D3N": ["Variável", "Produto Interno Bruto a preços correntes"],
    })


# ---- Testes básicos de importabilidade e instanciação ----


def test_ibge_extractor_importable():
    """Verifica que o extrator é importável."""
    assert IBGEExtractor is not None


def test_ibge_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado com defaults."""
    extractor = IBGEExtractor()
    assert extractor.timeout == 30
    assert extractor.max_retries == 3


# ---- Testes de cálculo correto ----


def test_extract_periodo_anual_retorna_vab_correto(ibge_extractor, ibge_sidra_5938_2022_response):
    """Mock de sidrapy.get_table() retornando PIB MA 2022.

    PIB = 139789146 mil reais → 139789.146 milhões R$
    VAB = PIB × 0.8932 = 124859.665... milhões R$
    """
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado == _VAB_ESPERADO


def test_extract_retorna_decimal_nao_float(ibge_extractor, ibge_sidra_5938_2022_response):
    """Verifica que o tipo de retorno é Decimal, não float."""
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert not isinstance(resultado, float)


def test_extract_periodo_trimestral_divide_por_4(
    ibge_extractor, ibge_sidra_5938_2022_response
):
    """Mock retornando PIB anual 2022; verifica VAB/4 para T1."""
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))

    esperado = _VAB_ESPERADO / Decimal("4")
    assert resultado == esperado


def test_extract_converte_pib_para_vab(
    ibge_extractor, ibge_sidra_5938_2022_response
):
    """Verifica que a conversão PIB → VAB está correta.

    PIB = 139789146 mil reais → 139789.146 milhões R$ → VAB = PIB × 0.8932
    """
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    pib_milhoes = Decimal(_PIB_MIL_REAIS) / Decimal("1000")
    assert resultado == pib_milhoes * _FATOR_VAB_PIB


# ---- Testes de erro e retry ----


def test_extract_api_indisponivel_levanta_extraction_error(ibge_extractor):
    """Mock de sidrapy.get_table() lançando ConnectionError; verifica ExtractionError."""
    with patch(
        "sidrapy.get_table",
        side_effect=requests.exceptions.ConnectionError("Conexão recusada"),
    ):
        with patch("time.sleep"):
            with pytest.raises(ExtractionError) as exc_info:
                ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert "IBGE SIDRA API indisponível" in str(exc_info.value)


def test_extract_retry_3_tentativas(ibge_extractor, ibge_sidra_5938_2022_response):
    """Mock falhando nas 2 primeiras e retornando sucesso na 3ª tentativa."""
    efeitos = [
        requests.exceptions.ConnectionError("Falha 1"),
        requests.exceptions.ConnectionError("Falha 2"),
        ibge_sidra_5938_2022_response,
    ]

    with patch("sidrapy.get_table", side_effect=efeitos):
        with patch("time.sleep"):
            resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado == _VAB_ESPERADO


def test_extract_falha_apos_max_retries(ibge_extractor):
    """Mock falhando em todas as 3 tentativas; verifica ExtractionError."""
    with patch(
        "sidrapy.get_table",
        side_effect=requests.exceptions.ConnectionError("Falha persistente"),
    ):
        with patch("time.sleep"):
            with pytest.raises(ExtractionError):
                ibge_extractor.extract(PeriodoCalculo(ano=2022))


def test_extract_backoff_exponencial(ibge_extractor):
    """Verifica que o backoff exponencial é aplicado corretamente (1s, 2s) entre retries."""
    with patch(
        "sidrapy.get_table",
        side_effect=requests.exceptions.ConnectionError("Falha"),
    ):
        with patch("time.sleep") as mock_sleep:
            with pytest.raises(ExtractionError):
                ibge_extractor.extract(PeriodoCalculo(ano=2022))

    # Com max_retries=3: sleep após tentativa 1 (1s) e tentativa 2 (2s)
    # Após tentativa 3 (última), não há sleep — lança ExtractionError
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0] == call(1)
    assert mock_sleep.call_args_list[1] == call(2)


def test_extract_numero_correto_de_tentativas(ibge_extractor):
    """Verifica que sidrapy.get_table() é chamado exatamente max_retries vezes em falha total."""
    with patch(
        "sidrapy.get_table",
        side_effect=requests.exceptions.ConnectionError("Falha"),
    ) as mock_get:
        with patch("time.sleep"):
            with pytest.raises(ExtractionError):
                ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert mock_get.call_count == ibge_extractor.max_retries


def test_extract_resposta_sem_dados_levanta_extraction_error(ibge_extractor):
    """Mock retornando DataFrame com apenas cabeçalho (sem dados); verifica ExtractionError."""
    # DataFrame com apenas 1 linha (cabeçalho de metadados, sem dados reais)
    resposta_vazia = pd.DataFrame({
        "NC": ["Nível Territorial (Código)"],
        "V": ["Valor"],
    })

    with patch("sidrapy.get_table", return_value=resposta_vazia):
        with pytest.raises(ExtractionError) as exc_info:
            ibge_extractor.extract(PeriodoCalculo(ano=2023))

    assert "IBGE SIDRA não retornou VAB" in str(exc_info.value)


def test_extract_value_error_levanta_extraction_error(ibge_extractor):
    """Mock de sidrapy.get_table() lançando ValueError; verifica ExtractionError.

    sidrapy levanta ValueError quando a API IBGE retorna HTTP >= 400.
    """
    with patch(
        "sidrapy.get_table",
        side_effect=ValueError("HTTP 404 Not Found"),
    ):
        with patch("time.sleep"):
            with pytest.raises(ExtractionError) as exc_info:
                ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert "IBGE SIDRA API indisponível" in str(exc_info.value)
