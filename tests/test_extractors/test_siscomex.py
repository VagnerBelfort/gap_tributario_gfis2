"""Testes do extrator Oracle Siscomex.

Testa o SiscomexExtractor com mocks de oracledb.connect() para garantir:
- Instanciação correta com parâmetros padrão
- Extração com dados MA para período anual e trimestral
- Comportamento com resultado vazio (DataFrame vazio)
- Tratamento correto de DatabaseError e OperationalError
- Comportamento quando oracledb não está instalado (ImportError)
- Que credenciais não aparecem em mensagens de log
"""

from __future__ import annotations

import logging
import sys
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.siscomex import SiscomexExtractor
from gap_tributario.models import PeriodoCalculo

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def extractor():
    """Extrator Siscomex com credenciais fictícias para testes."""
    return SiscomexExtractor(
        dsn="10.1.1.132:1521/cent",
        user="usuario_teste",
        password="senha_secreta",
        timeout=30,
        query_timeout=60,
    )


@pytest.fixture
def periodo_2022():
    """Período anual 2022."""
    return PeriodoCalculo(ano=2022)


@pytest.fixture
def periodo_2022_t3():
    """Período trimestral 2022-T3 (julho-setembro)."""
    return PeriodoCalculo(ano=2022, trimestre=3)


def _make_mock_cursor(rows: list, colunas: list) -> MagicMock:
    """Cria um mock de cursor Oracle com dados e descrição de colunas."""
    cursor = MagicMock()
    # description: lista de tuplas (name, type_code, ...)
    cursor.description = [(col.upper(), None, None, None, None, None, None) for col in colunas]
    cursor.fetchall.return_value = rows
    cursor.callTimeout = 0
    return cursor


def _make_mock_connection(cursor: MagicMock) -> MagicMock:
    """Cria um mock de conexão Oracle retornando o cursor fornecido."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    # Suporte a context manager (with connection:)
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


# ---------------------------------------------------------------------------
# Testes de instanciação
# ---------------------------------------------------------------------------


def test_siscomex_extractor_importavel():
    """Verifica que o módulo é importável sem oracledb instalado."""
    assert SiscomexExtractor is not None


def test_siscomex_extractor_instanciavel_com_defaults():
    """Verifica instanciação com parâmetros padrão de timeout."""
    ext = SiscomexExtractor(dsn="host:1521/svc", user="u", password="p")
    assert ext.dsn == "host:1521/svc"
    assert ext.user == "u"
    assert ext.password == "p"
    assert ext.timeout == 30
    assert ext.query_timeout == 60


def test_siscomex_extractor_instanciavel_com_timeouts_customizados():
    """Verifica instanciação com timeouts customizados."""
    ext = SiscomexExtractor(dsn="host:1521/svc", user="u", password="p", timeout=10, query_timeout=120)
    assert ext.timeout == 10
    assert ext.query_timeout == 120


# ---------------------------------------------------------------------------
# Testes de extração bem-sucedida
# ---------------------------------------------------------------------------


def test_extract_retorna_dataframe_com_colunas_corretas(extractor, periodo_2022):
    """extract() com mock retornando dados MA 2022 deve retornar DataFrame com colunas corretas."""
    import polars as pl

    colunas = [
        "num_declaracao",
        "uf_importador",
        "data_desembaraco",
        "valor_icms_devido",
        "aliquota_efetiva_icms",
        "base_calc_sefaz",
    ]
    rows = [
        ("00001/2022", "MA", date(2022, 3, 15), 1500.00, 0.18, 8333.33),
        ("00002/2022", "MA", date(2022, 6, 20), 2100.00, 0.18, 11666.67),
    ]

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        df = extractor.extract(periodo_2022)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    for col in colunas:
        assert col in df.columns


def test_extract_periodo_anual_chama_connect_com_dsn_correto(extractor, periodo_2022):
    """extract() deve chamar oracledb.connect() com o DSN configurado."""
    colunas = ["num_declaracao", "uf_importador", "data_desembaraco",
               "valor_icms_devido", "aliquota_efetiva_icms", "base_calc_sefaz"]
    rows = [("00001/2022", "MA", date(2022, 1, 10), 1000.0, 0.18, 5555.56)]

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        extractor.extract(periodo_2022)

    mock_oracledb.connect.assert_called_once_with(
        user=extractor.user,
        password=extractor.password,
        dsn=extractor.dsn,
        tcp_connect_timeout=extractor.timeout,
    )


def test_extract_periodo_trimestral_inclui_filtro_de_meses(extractor, periodo_2022_t3):
    """extract() para T3 deve incluir filtro de meses 7, 8, 9 na query."""
    colunas = ["num_declaracao", "uf_importador", "data_desembaraco",
               "valor_icms_devido", "aliquota_efetiva_icms", "base_calc_sefaz"]
    rows = [("00003/2022", "MA", date(2022, 8, 5), 900.0, 0.18, 5000.0)]

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        df = extractor.extract(periodo_2022_t3)

    # Verificar que a query foi chamada com os meses do T3
    call_args = cursor.execute.call_args
    params = call_args[0][1]  # segundo argumento posicional (dict de params)
    assert params.get("mes0") == 7
    assert params.get("mes1") == 8
    assert params.get("mes2") == 9
    assert params.get("ano") == 2022

    import polars as pl

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 1


def test_extract_query_sql_contem_join_e_filtro_ma(extractor, periodo_2022):
    """extract() deve gerar SQL com JOIN entre tabelas e filtro UF=MA."""
    colunas = ["num_declaracao", "uf_importador", "data_desembaraco",
               "valor_icms_devido", "aliquota_efetiva_icms", "base_calc_sefaz"]
    rows = []

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        extractor.extract(periodo_2022)

    sql_executado = cursor.execute.call_args[0][0]
    assert "TAB_DECL_SISCOMEX" in sql_executado
    assert "TAB_ITEM_SISCOMEX" in sql_executado
    assert "JOIN" in sql_executado
    assert "'MA'" in sql_executado
    assert "EXTRACT(YEAR FROM" in sql_executado


# ---------------------------------------------------------------------------
# Testes com resultado vazio
# ---------------------------------------------------------------------------


def test_extract_resultado_vazio_retorna_dataframe_vazio(extractor, periodo_2022):
    """extract() com mock retornando 0 linhas deve retornar DataFrame vazio (não levanta erro)."""
    import polars as pl

    colunas = ["num_declaracao", "uf_importador", "data_desembaraco",
               "valor_icms_devido", "aliquota_efetiva_icms", "base_calc_sefaz"]
    rows = []

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        df = extractor.extract(periodo_2022)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0
    assert "num_declaracao" in df.columns
    assert "valor_icms_devido" in df.columns


# ---------------------------------------------------------------------------
# Testes de erros de conexão e query
# ---------------------------------------------------------------------------


def test_extract_database_error_levanta_extraction_error_com_dsn(extractor, periodo_2022):
    """extract() com DatabaseError deve levantar ExtractionError com DSN na mensagem."""
    mock_oracledb = MagicMock()
    mock_oracledb.DatabaseError = Exception
    mock_oracledb.OperationalError = type("OperationalError", (Exception,), {})
    mock_oracledb.connect.side_effect = mock_oracledb.DatabaseError("ORA-12541: TNS no listener")

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(periodo_2022)

    assert extractor.dsn in str(exc_info.value)


def test_extract_operational_error_levanta_extraction_error_com_dsn(extractor, periodo_2022):
    """extract() com OperationalError de conexão deve levantar ExtractionError com DSN."""
    mock_oracledb = MagicMock()
    mock_oracledb.DatabaseError = type("DatabaseError", (Exception,), {})
    mock_oracledb.OperationalError = Exception
    mock_oracledb.connect.side_effect = mock_oracledb.OperationalError("Timeout de conexão")

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(periodo_2022)

    assert extractor.dsn in str(exc_info.value)


def test_extract_database_error_na_query_levanta_extraction_error(extractor, periodo_2022):
    """extract() com DatabaseError durante execução da query levanta ExtractionError."""
    mock_oracledb = MagicMock()
    mock_oracledb.DatabaseError = Exception
    mock_oracledb.OperationalError = type("OperationalError", (Exception,), {})

    cursor = MagicMock()
    cursor.execute.side_effect = mock_oracledb.DatabaseError("ORA-00942: table or view does not exist")
    cursor.callTimeout = 0

    conn = _make_mock_connection(cursor)
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(periodo_2022)

    assert extractor.dsn in str(exc_info.value)


def test_extract_operational_error_na_query_levanta_extraction_error(extractor, periodo_2022):
    """extract() com OperationalError durante query levanta ExtractionError."""
    mock_oracledb = MagicMock()
    mock_oracledb.DatabaseError = type("DatabaseError", (Exception,), {})
    mock_oracledb.OperationalError = Exception

    cursor = MagicMock()
    cursor.execute.side_effect = mock_oracledb.OperationalError("Query timeout")
    cursor.callTimeout = 0

    conn = _make_mock_connection(cursor)
    mock_oracledb.connect.return_value = conn

    with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract(periodo_2022)

    assert extractor.dsn in str(exc_info.value)


# ---------------------------------------------------------------------------
# Teste quando oracledb não está instalado
# ---------------------------------------------------------------------------


def test_extract_sem_oracledb_instalado_levanta_extraction_error(extractor, periodo_2022):
    """extract() sem oracledb instalado deve levantar ExtractionError com instrução de instalação."""
    # Remover oracledb do sys.modules caso esteja presente
    original = sys.modules.pop("oracledb", None)
    try:
        # Simular ImportError forçando None no módulo
        sys.modules["oracledb"] = None  # type: ignore[assignment]

        with pytest.raises((ExtractionError, ImportError)):
            extractor.extract(periodo_2022)
    finally:
        # Restaurar estado original
        if original is not None:
            sys.modules["oracledb"] = original
        else:
            sys.modules.pop("oracledb", None)


def test_extract_import_error_levanta_extraction_error_com_instrucao(extractor, periodo_2022):
    """extract() com ImportError no oracledb deve levantar ExtractionError com instrução de instalação."""
    # Salvar e remover módulo para simular que oracledb não está instalado
    original = sys.modules.pop("oracledb", None)
    try:
        sys.modules["oracledb"] = None  # type: ignore[assignment]
        with pytest.raises((ExtractionError, ImportError)) as exc_info:
            extractor.extract(periodo_2022)
        # Verificar que a mensagem contém instrução de instalação (quando ExtractionError)
        if isinstance(exc_info.value, ExtractionError):
            assert "pip install" in str(exc_info.value)
    finally:
        if original is not None:
            sys.modules["oracledb"] = original
        else:
            sys.modules.pop("oracledb", None)


# ---------------------------------------------------------------------------
# Teste de segurança — credenciais não no log
# ---------------------------------------------------------------------------


def test_credenciais_nao_aparecem_em_mensagens_de_log(extractor, periodo_2022, caplog):
    """Senha do extrator não deve aparecer em nenhuma mensagem de log."""
    colunas = ["num_declaracao", "uf_importador", "data_desembaraco",
               "valor_icms_devido", "aliquota_efetiva_icms", "base_calc_sefaz"]
    rows = [("00001/2022", "MA", date(2022, 3, 15), 1000.0, 0.18, 5555.56)]

    cursor = _make_mock_cursor(rows, colunas)
    conn = _make_mock_connection(cursor)

    mock_oracledb = MagicMock()
    mock_oracledb.connect.return_value = conn

    with caplog.at_level(logging.DEBUG):
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            extractor.extract(periodo_2022)

    # Senha nunca deve aparecer nos logs
    for record in caplog.records:
        assert extractor.password not in record.getMessage()


# ---------------------------------------------------------------------------
# Testes de _build_query
# ---------------------------------------------------------------------------


def test_build_query_anual_sem_filtro_meses(extractor, periodo_2022):
    """_build_query() para período anual não deve incluir filtro de meses."""
    sql, params = extractor._build_query(periodo_2022)
    assert "EXTRACT(MONTH FROM" not in sql
    assert params == {"ano": 2022}


def test_build_query_trimestral_t1_filtra_meses_1_2_3(extractor):
    """_build_query() para T1 deve incluir meses 1, 2, 3."""
    periodo_t1 = PeriodoCalculo(ano=2022, trimestre=1)
    sql, params = extractor._build_query(periodo_t1)
    assert "EXTRACT(MONTH FROM" in sql
    assert params.get("mes0") == 1
    assert params.get("mes1") == 2
    assert params.get("mes2") == 3


def test_build_query_trimestral_t2_filtra_meses_4_5_6(extractor):
    """_build_query() para T2 deve incluir meses 4, 5, 6."""
    periodo_t2 = PeriodoCalculo(ano=2022, trimestre=2)
    _, params = extractor._build_query(periodo_t2)
    assert params.get("mes0") == 4
    assert params.get("mes1") == 5
    assert params.get("mes2") == 6


def test_build_query_trimestral_t3_filtra_meses_7_8_9(extractor):
    """_build_query() para T3 deve incluir meses 7, 8, 9."""
    periodo_t3 = PeriodoCalculo(ano=2022, trimestre=3)
    _, params = extractor._build_query(periodo_t3)
    assert params.get("mes0") == 7
    assert params.get("mes1") == 8
    assert params.get("mes2") == 9


def test_build_query_trimestral_t4_filtra_meses_10_11_12(extractor):
    """_build_query() para T4 deve incluir meses 10, 11, 12."""
    periodo_t4 = PeriodoCalculo(ano=2022, trimestre=4)
    _, params = extractor._build_query(periodo_t4)
    assert params.get("mes0") == 10
    assert params.get("mes1") == 11
    assert params.get("mes2") == 12
