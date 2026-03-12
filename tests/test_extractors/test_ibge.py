"""Testes do extrator IBGE SIDRA.

Testa o IBGEExtractor com mocks de sidrapy.get_table() para garantir:
- Parsing correto do VAB em milhões R$
- Conversão de unidade (mil reais → milhões R$)
- Mecanismo de retry com backoff exponencial
- Tratamento correto de erros
- Divisão por 4 para períodos trimestrais
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import call, patch

import pytest
import requests

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.ibge import IBGEExtractor
from gap_tributario.models import PeriodoCalculo

# Caminho para o fixture de resposta da API
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "api_responses"


@pytest.fixture
def ibge_extractor():
    """Extrator IBGE com timeout curto para testes."""
    return IBGEExtractor(timeout=5, max_retries=3)


@pytest.fixture
def ibge_sidra_5938_2022_response():
    """Resposta da API IBGE SIDRA para 2022 (fixture em JSON)."""
    fixture_path = FIXTURE_DIR / "ibge_sidra_5938_2022.json"
    with open(fixture_path) as f:
        return json.load(f)


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
    """Mock de sidrapy.get_table() retornando fixture ibge_sidra_5938_2022.json.

    Valor esperado: 124859000 mil reais / 1000 = 124859.000 milhões R$
    """
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado == Decimal("124859.000")


def test_extract_retorna_decimal_nao_float(ibge_extractor, ibge_sidra_5938_2022_response):
    """Verifica que o tipo de retorno é Decimal, não float."""
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert not isinstance(resultado, float)


def test_extract_periodo_trimestral_divide_por_4(
    ibge_extractor, ibge_sidra_5938_2022_response
):
    """Mock retornando VAB anual 2022; verifica divisão por 4 para T1."""
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))

    esperado = Decimal("124859.000") / Decimal("4")
    assert resultado == esperado


def test_extract_converte_mil_reais_para_milhoes(
    ibge_extractor, ibge_sidra_5938_2022_response
):
    """Verifica que a conversão de unidade está correta.

    Fixture retorna '124859000' em mil reais → deve retornar Decimal('124859.000') em milhões.
    """
    with patch("sidrapy.get_table", return_value=ibge_sidra_5938_2022_response):
        resultado = ibge_extractor.extract(PeriodoCalculo(ano=2022))

    # 124859000 (mil reais) / 1000 = 124859.000 (milhões R$)
    assert resultado == Decimal("124859000") / Decimal("1000")


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
    assert resultado == Decimal("124859.000")


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


def test_extract_resposta_sem_valor_levanta_extraction_error(ibge_extractor):
    """Mock retornando resposta sem o ano solicitado na série; verifica ExtractionError."""
    resposta_sem_ano = [
        {
            "id": "5938",
            "variavel": "Valor adicionado bruto a preços correntes",
            "unidade": "Mil reais",
            "resultados": [
                {
                    "classificacoes": [],
                    "series": [
                        {
                            "localidade": {
                                "id": "21",
                                "nivel": {"id": "N3", "nome": "Unidade da Federação"},
                                "nome": "Maranhão",
                            },
                            "serie": {"2022": "124859000"},  # não contém 2023
                        }
                    ],
                }
            ],
        }
    ]

    with patch("sidrapy.get_table", return_value=resposta_sem_ano):
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
