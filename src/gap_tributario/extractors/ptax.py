"""Extrator da cotação do dólar via API BCB PTAX.

Responsável por consultar a API REST do Banco Central do Brasil
e retornar a cotação média PTAX para o período especificado.

Endpoint:
    GET https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/
        CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)

Retorno: Média aritmética de cotacaoVenda de todos os dias úteis do período
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar extrator BCB PTAX cotação do dólar"


class PTAXExtractor:
    """Extrator da cotação do dólar via API BCB PTAX."""

    BCB_BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"

    def __init__(self, timeout: int = 15, max_retries: int = 3) -> None:
        """Inicializa o extrator com configurações de timeout e retry.

        Args:
            timeout: Timeout em segundos para requisições à API (default: 15)
            max_retries: Número máximo de tentativas (default: 3)
        """
        self.timeout = timeout
        self.max_retries = max_retries

    def extract(self, periodo: object) -> object:
        """Extrai a cotação média PTAX para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com a cotação média PTAX em R$/USD

        Raises:
            ExtractionError: Se a API BCB estiver indisponível após retries
        """
        raise NotImplementedError(
            "PTAXExtractor não implementado. "
            "Implementar na tarefa 'Implementar extrator BCB PTAX cotação do dólar'"
        )
