"""Extrator do Valor Adicionado Bruto (VAB) via IBGE SIDRA API.

Responsável por consultar a API IBGE SIDRA (Tabela 5938)
e retornar o VAB do Maranhão para o período especificado.

A variável 37 da tabela 5938 retorna o **PIB** a preços correntes (não o VAB
diretamente). Como a API SIDRA não disponibiliza o VAB total (var 498) para
nível UF, convertemos PIB → VAB subtraindo os impostos líquidos sobre produtos.

A razão VAB/PIB para o Maranhão gira em torno de 0.893 (referência: Contas
Regionais IBGE 2022, VAB=124.859M / PIB=139.789M). Essa proporção é estável
ao longo dos anos para estados com perfil econômico similar.

Parâmetros SIDRA:
- table_code: "5938" (Contas Regionais)
- territorial_level: "3" (UF)
- ibge_territorial_code: "21" (Maranhão)
- variable: "37" (PIB a preços correntes)
- period: "{ano}" (anual)

Conversão: VAB = PIB × FATOR_VAB_PIB
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

# Fator de conversão PIB → VAB para o Maranhão.
# Derivado das Contas Regionais IBGE 2022: VAB=124.859M / PIB=139.789M = 0.8932
# Esse fator desconta os "impostos, líquidos de subsídios, sobre produtos"
# que compõem a diferença entre PIB e VAB.
_FATOR_VAB_PIB = Decimal("0.8932")


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
            # sidrapy.get_table() retorna um DataFrame pandas onde:
            # - linha 0 = cabeçalhos de metadados
            # - linha 1+ = dados reais
            # - coluna "V" = valor numérico (em Mil Reais para tabela 5938)
            if len(dados) < 2:
                raise ValueError("DataFrame retornado sem dados (apenas cabeçalho)")
            valor_str = dados.iloc[1]["V"]
            if valor_str in (None, "", "-", "...", "..."):
                raise ValueError(f"Valor indisponível na API: '{valor_str}'")
            # A variável 37 retorna o PIB (não VAB). Convertemos:
            # PIB (em Mil Reais) → milhões → VAB (aplicando fator)
            pib_anual = Decimal(valor_str) / Decimal("1000")
            vab_anual = pib_anual * _FATOR_VAB_PIB
            logger.info(
                "PIB Maranhão %s: R$ %.3f milhões → VAB (×%.4f): R$ %.3f milhões",
                ano_str,
                float(pib_anual),
                float(_FATOR_VAB_PIB),
                float(vab_anual),
            )
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
