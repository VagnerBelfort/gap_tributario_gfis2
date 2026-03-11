"""Testes do extrator BCB PTAX.

Testa o PTAXExtractor com mocks de httpx.get() para garantir:
- Cálculo correto da média PTAX
- Formato correto das datas (MM-DD-YYYY)
- Mecanismo de retry com backoff exponencial
- Tratamento correto de erros
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.ptax import PTAXExtractor
from gap_tributario.models import PeriodoCalculo

# Caminho para o fixture de resposta da API
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "api_responses"


@pytest.fixture
def ptax_extractor():
    """Extrator PTAX com timeout curto para testes."""
    return PTAXExtractor(timeout=5, max_retries=3)


@pytest.fixture
def bcb_ptax_2022_response():
    """Resposta da API BCB PTAX para 2022 (fixture em JSON)."""
    fixture_path = FIXTURE_DIR / "bcb_ptax_2022.json"
    with open(fixture_path) as f:
        return json.load(f)


def _make_mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Cria um mock de resposta httpx."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_data
    mock_resp.status_code = status_code
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}",
            request=MagicMock(),
            response=mock_resp,
        )
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp


# ---- Testes básicos de importabilidade e instanciação ----


def test_ptax_extractor_importable():
    """Verifica que o extrator é importável."""
    assert PTAXExtractor is not None


def test_ptax_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado com defaults."""
    extractor = PTAXExtractor()
    assert extractor.timeout == 15
    assert extractor.max_retries == 3


# ---- Testes de cálculo correto ----


def test_extract_periodo_anual_retorna_media_correta(ptax_extractor, bcb_ptax_2022_response):
    """Mock de httpx.get() retornando o conteúdo de bcb_ptax_2022.json.

    Média esperada: (5.5700 + 5.4882 + 5.3970 + 5.3060 + 5.2160) / 5 = 5.39544
    """
    mock_resp = _make_mock_response(bcb_ptax_2022_response)

    with patch("httpx.get", return_value=mock_resp):
        resultado = ptax_extractor.extract(PeriodoCalculo(ano=2022))

    # Verificar que é Decimal
    assert isinstance(resultado, Decimal)

    # Calcular média esperada manualmente
    cotacoes = [Decimal(str(v["cotacaoVenda"])) for v in bcb_ptax_2022_response["value"]]
    media_esperada = sum(cotacoes) / Decimal(len(cotacoes))

    assert resultado == media_esperada


def test_extract_retorna_decimal_nao_float(ptax_extractor, bcb_ptax_2022_response):
    """Verifica que o tipo de retorno é Decimal, não float."""
    mock_resp = _make_mock_response(bcb_ptax_2022_response)

    with patch("httpx.get", return_value=mock_resp):
        resultado = ptax_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert not isinstance(resultado, float)


# ---- Testes de formato de datas ----


def test_extract_formato_datas_mm_dd_yyyy(ptax_extractor, bcb_ptax_2022_response):
    """Verifica que as datas na URL seguem o formato MM-DD-YYYY e não YYYY-MM-DD."""
    mock_resp = _make_mock_response(bcb_ptax_2022_response)

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        ptax_extractor.extract(PeriodoCalculo(ano=2022))

    url_chamada = mock_get.call_args[0][0]
    # O formato correto é MM-DD-YYYY: '01-01-2022'
    assert "'01-01-2022'" in url_chamada
    assert "'12-31-2022'" in url_chamada
    # Garantir que NÃO está no formato YYYY-MM-DD
    assert "'2022-01-01'" not in url_chamada
    assert "'2022-12-31'" not in url_chamada


def test_extract_periodo_trimestral_datas_corretas(ptax_extractor, bcb_ptax_2022_response):
    """Verifica que a URL para T1/2022 usa datas 01-01-2022 a 03-31-2022."""
    mock_resp = _make_mock_response(bcb_ptax_2022_response)

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        ptax_extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))

    url_chamada = mock_get.call_args[0][0]
    assert "'01-01-2022'" in url_chamada
    assert "'03-31-2022'" in url_chamada


# ---- Testes de intervalos de trimestres ----


def test_trimestre_t1_datas_janeiro_marco(ptax_extractor):
    """Verifica intervalo 01-01-YYYY a 03-31-YYYY para T1."""
    data_inicio, data_fim = ptax_extractor._build_date_range(PeriodoCalculo(ano=2022, trimestre=1))
    assert data_inicio == "01-01-2022"
    assert data_fim == "03-31-2022"


def test_trimestre_t2_datas_abril_junho(ptax_extractor):
    """Verifica intervalo 04-01-YYYY a 06-30-YYYY para T2."""
    data_inicio, data_fim = ptax_extractor._build_date_range(PeriodoCalculo(ano=2022, trimestre=2))
    assert data_inicio == "04-01-2022"
    assert data_fim == "06-30-2022"


def test_trimestre_t3_datas_julho_setembro(ptax_extractor):
    """Verifica intervalo 07-01-YYYY a 09-30-YYYY para T3."""
    data_inicio, data_fim = ptax_extractor._build_date_range(PeriodoCalculo(ano=2022, trimestre=3))
    assert data_inicio == "07-01-2022"
    assert data_fim == "09-30-2022"


def test_trimestre_t4_datas_outubro_dezembro(ptax_extractor):
    """Verifica intervalo 10-01-YYYY a 12-31-YYYY para T4."""
    data_inicio, data_fim = ptax_extractor._build_date_range(PeriodoCalculo(ano=2022, trimestre=4))
    assert data_inicio == "10-01-2022"
    assert data_fim == "12-31-2022"


# ---- Testes de URL ----


def test_build_url_contem_parametros_corretos(ptax_extractor):
    """Verifica que a URL montada contém todos os parâmetros OData necessários."""
    url = ptax_extractor._build_url("01-01-2022", "12-31-2022")

    assert "olinda.bcb.gov.br" in url
    assert "CotacaoDolarPeriodo" in url
    assert "@di='01-01-2022'" in url
    assert "@df='12-31-2022'" in url
    assert "$format=json" in url
    assert "$select=cotacaoVenda,dataHoraCotacao" in url


# ---- Testes de erro e retry ----


def test_extract_api_indisponivel_levanta_extraction_error(ptax_extractor):
    """Mock de httpx.get() lançando RequestError; verifica ExtractionError após retries."""
    with patch("httpx.get", side_effect=httpx.RequestError("Conexão recusada")):
        with patch("time.sleep"):  # Não aguardar de fato nos testes
            with pytest.raises(ExtractionError) as exc_info:
                ptax_extractor.extract(PeriodoCalculo(ano=2022))

    assert "BCB PTAX API indisponível" in str(exc_info.value)
    assert "--ptax-manual" in str(exc_info.value)


def test_extract_resposta_vazia_levanta_extraction_error(ptax_extractor):
    """Mock retornando {'value': []}; verifica ExtractionError com mensagem clara."""
    mock_resp = _make_mock_response({"value": []})

    with patch("httpx.get", return_value=mock_resp):
        with pytest.raises(ExtractionError) as exc_info:
            ptax_extractor.extract(PeriodoCalculo(ano=2022))

    assert "lista vazia" in str(exc_info.value)


def test_extract_retry_3_tentativas(ptax_extractor, bcb_ptax_2022_response):
    """Mock falhando nas 2 primeiras e retornando sucesso na 3ª tentativa."""
    mock_sucesso = _make_mock_response(bcb_ptax_2022_response)
    efeitos = [
        httpx.RequestError("Falha 1"),
        httpx.RequestError("Falha 2"),
        mock_sucesso,
    ]

    with patch("httpx.get", side_effect=efeitos):
        with patch("time.sleep"):
            resultado = ptax_extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado > Decimal("0")


def test_extract_falha_apos_max_retries(ptax_extractor):
    """Mock falhando em todas as 3 tentativas; verifica ExtractionError."""
    with patch("httpx.get", side_effect=httpx.RequestError("Falha persistente")):
        with patch("time.sleep"):
            with pytest.raises(ExtractionError):
                ptax_extractor.extract(PeriodoCalculo(ano=2022))


def test_extract_http_error_status_code(ptax_extractor):
    """Mock retornando resposta com status 500; verifica retry e erro final."""
    mock_resp_500 = _make_mock_response({}, status_code=500)

    with patch("httpx.get", return_value=mock_resp_500):
        with patch("time.sleep"):
            with pytest.raises(ExtractionError):
                ptax_extractor.extract(PeriodoCalculo(ano=2022))


def test_extract_numero_correto_de_tentativas(ptax_extractor):
    """Verifica que httpx.get() é chamado exatamente max_retries vezes em caso de falha."""
    with patch("httpx.get", side_effect=httpx.RequestError("Falha")) as mock_get:
        with patch("time.sleep"):
            with pytest.raises(ExtractionError):
                ptax_extractor.extract(PeriodoCalculo(ano=2022))

    assert mock_get.call_count == ptax_extractor.max_retries


def test_extract_backoff_exponencial(ptax_extractor):
    """Verifica que o backoff exponencial é aplicado corretamente (1s, 2s) entre retries."""
    with patch("httpx.get", side_effect=httpx.RequestError("Falha")):
        with patch("time.sleep") as mock_sleep:
            with pytest.raises(ExtractionError):
                ptax_extractor.extract(PeriodoCalculo(ano=2022))

    # Com max_retries=3: sleep após tentativa 1 (1s) e tentativa 2 (2s)
    # Após tentativa 3 (última), não há sleep — lança ExtractionError
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0] == call(1)
    assert mock_sleep.call_args_list[1] == call(2)
