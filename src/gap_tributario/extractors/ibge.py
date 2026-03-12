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

import logging
import time
from decimal import Decimal

import requests
import sidrapy

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)


class IBGEExtractor:
    """Extrator do VAB via IBGE SIDRA API."""

    def __init__(self, timeout: int = 30, max_retries: int = 3) -> None:
        """Inicializa o extrator com configurações de timeout e retry.

        Args:
            timeout: Timeout em segundos (armazenado para consistência de interface;
                     a biblioteca sidrapy não expõe parâmetro de timeout em get_table())
            max_retries: Número máximo de tentativas (default: 3)
        """
        self.timeout = timeout
        self.max_retries = max_retries

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai o VAB do Maranhão para o período.

        Consulta a API IBGE SIDRA (Tabela 5938) e retorna o VAB do Maranhão
        em milhões de R$. Para períodos trimestrais, aplica divisão por 4
        conforme metodologia OCDE. Implementa retry com backoff exponencial
        (1s, 2s, 4s) em caso de falha.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com o VAB em milhões R$

        Raises:
            ExtractionError: Se a API IBGE estiver indisponível após retries
                             ou se não houver dados para o período solicitado
        """
        ano_str = str(periodo.ano)

        logger.info(
            "Consultando IBGE SIDRA Tabela 5938 para VAB do Maranhão, ano %s",
            ano_str,
        )

        for tentativa in range(1, self.max_retries + 1):
            try:
                dados = sidrapy.get_table(
                    table_code="5938",
                    territorial_level="3",
                    ibge_territorial_code="21",
                    variable="37",
                    period=ano_str,
                )
                break
            except (ValueError, requests.exceptions.RequestException) as exc:
                if tentativa < self.max_retries:
                    espera = 2 ** (tentativa - 1)  # 1s, 2s, 4s
                    logger.warning(
                        "Tentativa %d/%d falhou para IBGE SIDRA %s. Aguardando %ds. Erro: %s",
                        tentativa,
                        self.max_retries,
                        ano_str,
                        espera,
                        exc,
                    )
                    time.sleep(espera)
                else:
                    raise ExtractionError(
                        f"IBGE SIDRA API indisponível para {ano_str} após {self.max_retries} "
                        f"tentativas. Erro: {exc}"
                    ) from exc

        try:
            valor_str = dados[0]["resultados"][0]["series"][0]["serie"][ano_str]
            vab_anual = Decimal(valor_str) / Decimal("1000")
        except (KeyError, IndexError, ValueError) as exc:
            raise ExtractionError(
                f"IBGE SIDRA não retornou VAB para o Maranhão no ano {ano_str}. "
                f"Resposta da API não contém o campo esperado. Erro: {exc}"
            ) from exc

        if periodo.is_anual:
            vab = vab_anual
        else:
            vab = vab_anual / Decimal("4")

        logger.info(
            "VAB Maranhão %s: R$ %.3f milhões (período: %s)",
            ano_str,
            float(vab),
            periodo.label,
        )

        return vab
