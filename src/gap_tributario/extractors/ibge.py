"""Extrator do Valor Adicionado Bruto (VAB) via IBGE SIDRA API.

Responsável por consultar a API IBGE SIDRA (Tabela 5938)
e retornar o VAB do Maranhão para o período especificado.

Parâmetros SIDRA:
- table_code: "5938" (Contas Regionais)
- territorial_level: "3" (UF)
- ibge_territorial_code: "21" (Maranhão)
- variable: "37" (VAB a preços correntes)
- period: "{ano}" (anual)

Para cálculo trimestral: VAB_trimestral = VAB_anual / 4
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar extrator IBGE SIDRA Valor Adicionado Bruto"


class IBGEExtractor:
    """Extrator do VAB via IBGE SIDRA API."""

    def __init__(self, timeout: int = 30, max_retries: int = 3) -> None:
        """Inicializa o extrator com configurações de timeout e retry.

        Args:
            timeout: Timeout em segundos para requisições à API (default: 30)
            max_retries: Número máximo de tentativas (default: 3)
        """
        self.timeout = timeout
        self.max_retries = max_retries

    def extract(self, periodo: object) -> object:
        """Extrai o VAB do Maranhão para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com o VAB em milhões R$

        Raises:
            ExtractionError: Se a API IBGE estiver indisponível após retries
        """
        raise NotImplementedError(
            "IBGEExtractor não implementado. "
            "Implementar na tarefa 'Implementar extrator IBGE SIDRA Valor Adicionado Bruto'"
        )
