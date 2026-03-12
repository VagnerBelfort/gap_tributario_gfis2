"""Extrator de dados do Oracle Siscomex (opcional).

Responsável por consultar o banco Oracle Siscomex (C3: 10.1.1.132:1521/cent)
para enriquecimento dos dados de ICMS das Declarações de Importação (DIs)
do Maranhão quando habilitado via --siscomex.

Configuração via env vars (em AppConfig):
- ORACLE_DSN: ex: 10.1.1.132:1521/cent
- ORACLE_USER: usuário Oracle
- ORACLE_PASSWORD: senha Oracle

Tabelas:
    APL_SISCOMEX.TAB_DECL_SISCOMEX  — dados da declaração (DI)
    APL_SISCOMEX.TAB_ITEM_SISCOMEX  — dados dos itens da DI
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    import polars as pl

    from gap_tributario.models import PeriodoCalculo

from gap_tributario.extractors.base import ExtractionError

logger = logging.getLogger(__name__)

# Mapeamento trimestre → meses correspondentes
_TRIMESTRE_MESES: dict = {
    1: (1, 2, 3),
    2: (4, 5, 6),
    3: (7, 8, 9),
    4: (10, 11, 12),
}

_SQL_BASE = """
SELECT
    d.TDS_NUM_DECL          AS num_declaracao,
    d.TDS_UF_IMPORTADOR     AS uf_importador,
    d.TDS_DATA_DESEMBARACO  AS data_desembaraco,
    i.TDI_VALOR_DEVIDO_ICMS AS valor_icms_devido,
    i.TDI_ALIQ_EFETIVA_ICMS AS aliquota_efetiva_icms,
    i.TDI_BASE_CALC_SEFAZ   AS base_calc_sefaz
FROM
    APL_SISCOMEX.TAB_DECL_SISCOMEX d
    JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
        ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL
WHERE
    d.TDS_UF_IMPORTADOR = 'MA'
    AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) = :ano
    {filtro_meses}
"""


class SiscomexExtractor:
    """Extrator de dados do Oracle Siscomex (opcional).

    Consulta as tabelas APL_SISCOMEX.TAB_ITEM_SISCOMEX e
    APL_SISCOMEX.TAB_DECL_SISCOMEX no banco Oracle C3, filtrando pelas
    Declarações de Importação (DIs) do Maranhão para o período especificado.

    oracledb é dependência OPCIONAL — não importado no nível do módulo para
    não quebrar quando a dependência não está instalada.
    """

    def __init__(
        self,
        dsn: str,
        user: str,
        password: str,
        timeout: int = 30,
        query_timeout: int = 60,
    ) -> None:
        """Inicializa o extrator com configurações de conexão Oracle.

        Args:
            dsn: DSN Oracle (ex: host:porta/servico)
            user: Usuário Oracle
            password: Senha Oracle
            timeout: Timeout de conexão em segundos (default: 30)
            query_timeout: Timeout de query em segundos (default: 60)
        """
        self.dsn = dsn
        self.user = user
        self.password = password
        self.timeout = timeout
        self.query_timeout = query_timeout

    def _build_query(self, periodo: "PeriodoCalculo") -> Tuple[str, dict]:
        """Monta a query SQL e os parâmetros de bind para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Tupla (sql, params) onde params é dicionário de bind variables
        """
        params: dict = {"ano": periodo.ano}

        if periodo.is_anual:
            filtro_meses = ""
        else:
            meses: Tuple = _TRIMESTRE_MESES[periodo.trimestre]
            placeholders = ", ".join(f":mes{i}" for i in range(len(meses)))
            filtro_meses = f"AND EXTRACT(MONTH FROM d.TDS_DATA_DESEMBARACO) IN ({placeholders})"
            for i, mes in enumerate(meses):
                params[f"mes{i}"] = mes

        sql = _SQL_BASE.format(filtro_meses=filtro_meses)
        return sql, params

    def extract(self, periodo: "PeriodoCalculo") -> "pl.DataFrame":
        """Extrai dados do Siscomex Oracle para o período.

        Conecta ao banco Oracle em thin mode (sem Oracle Client), executa
        JOIN entre TAB_DECL_SISCOMEX e TAB_ITEM_SISCOMEX filtrando por
        UF=MA e período, e retorna pl.DataFrame com os dados de ICMS.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            DataFrame Polars com as colunas:
                - num_declaracao (str): número da DI
                - uf_importador (str): UF do importador (sempre 'MA')
                - data_desembaraco (datetime): data de desembaraço
                - valor_icms_devido (float): valor ICMS devido em R$
                - aliquota_efetiva_icms (float): alíquota efetiva ICMS
                - base_calc_sefaz (float): base de cálculo SEFAZ em R$

        Raises:
            ExtractionError: Se oracledb não estiver instalado, se Oracle
                estiver indisponível, ou se ocorrer erro na query (fail-fast)
        """
        try:
            import oracledb  # noqa: PLC0415 (lazy import intencional)
        except ImportError as exc:
            raise ExtractionError(
                "Dependência 'oracledb' não instalada. "
                "Instale com: pip install 'gap-tributario[oracle]' "
                "ou: pip install oracledb>=2.0.0"
            ) from exc

        try:
            import polars as pl  # noqa: PLC0415 (lazy import intencional)
        except ImportError as exc:
            raise ExtractionError(
                "Dependência 'polars' não instalada. Instale com: pip install polars"
            ) from exc

        logger.info(
            "Conectando ao Oracle Siscomex (DSN: %s) para período %s",
            self.dsn,
            periodo.label,
        )

        sql, params = self._build_query(periodo)

        try:
            connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
                tcp_connect_timeout=self.timeout,
            )
        except oracledb.DatabaseError as exc:
            raise ExtractionError(
                f"Falha ao conectar ao Oracle Siscomex (DSN: {self.dsn}). "
                f"Verifique as credenciais e a disponibilidade do servidor. "
                f"Erro: {exc}"
            ) from exc
        except oracledb.OperationalError as exc:
            raise ExtractionError(
                f"Erro operacional ao conectar ao Oracle Siscomex (DSN: {self.dsn}). "
                f"O servidor pode estar indisponível ou timeout excedido. "
                f"Erro: {exc}"
            ) from exc

        with connection:
            try:
                cursor = connection.cursor()
                cursor.callTimeout = self.query_timeout * 1000  # ms
                cursor.execute(sql, params)

                colunas = [col[0].lower() for col in cursor.description]
                linhas: List[tuple] = cursor.fetchall()

            except oracledb.DatabaseError as exc:
                raise ExtractionError(
                    f"Erro ao executar query no Oracle Siscomex (DSN: {self.dsn}). "
                    f"Erro: {exc}"
                ) from exc
            except oracledb.OperationalError as exc:
                raise ExtractionError(
                    f"Timeout ou erro operacional na query Oracle Siscomex (DSN: {self.dsn}). "
                    f"Erro: {exc}"
                ) from exc

        logger.info(
            "Oracle Siscomex: %d registros extraídos para %s",
            len(linhas),
            periodo.label,
        )

        if not linhas:
            logger.warning(
                "Oracle Siscomex retornou 0 registros para período %s "
                "(UF=MA, DSN=%s). Verifique se há dados disponíveis.",
                periodo.label,
                self.dsn,
            )
            schema = {
                "num_declaracao": pl.Utf8,
                "uf_importador": pl.Utf8,
                "data_desembaraco": pl.Date,
                "valor_icms_devido": pl.Float64,
                "aliquota_efetiva_icms": pl.Float64,
                "base_calc_sefaz": pl.Float64,
            }
            return pl.DataFrame(schema=schema)

        data: dict = {col: [row[i] for row in linhas] for i, col in enumerate(colunas)}
        df = pl.DataFrame(data)

        logger.info(
            "Siscomex: ICMS devido total = R$ %.2f milhões para %s",
            df["valor_icms_devido"].sum() / 1_000_000,
            periodo.label,
        )

        return df
