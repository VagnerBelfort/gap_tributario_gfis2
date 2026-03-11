"""Extrator de dados do Oracle Siscomex (opcional).

Responsável por consultar o banco Oracle Siscomex
para enriquecimento dos dados de importações quando habilitado via --siscomex.

Configuração:
- DSN: env var ORACLE_DSN (ex: 10.1.1.132:1521/cent)
- User: env var ORACLE_USER
- Password: env var ORACLE_PASSWORD

Tabelas: APL_SISCOMEX_DI_DETALHES, APL_SISCOMEX_DI_ADICOES
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar extrator Siscomex Oracle (opcional)"


class SiscomexExtractor:
    """Extrator de dados do Oracle Siscomex (opcional)."""

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

    def extract(self, periodo: object) -> object:
        """Extrai dados do Siscomex Oracle para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            DataFrame Polars com dados de ICMS Siscomex

        Raises:
            ExtractionError: Se Oracle estiver indisponível (fail-fast, não silencioso)
        """
        raise NotImplementedError(
            "SiscomexExtractor não implementado. "
            "Implementar na tarefa 'Implementar extrator Siscomex Oracle (opcional)'"
        )
