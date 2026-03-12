"""Motor de cálculo VRR (OCDE) para o Gap Tributário do ICMS-MA.

Implementa a fórmula:
    ICMS Potencial = (VAB - Exportações + Importações) × Alíquota Padrão
    VRR = ICMS Arrecadado / ICMS Potencial
    Gap Absoluto = ICMS Potencial - ICMS Arrecadado
    Gap Percentual = (Gap Absoluto / ICMS Potencial) × 100

Referência de validação MA 2022:
    ICMS=10.917, VAB=124.859, Exp=29.754, Imp=21.924, Alíq=0.18
    ICMS Potencial = (124.859 - 29.754 + 21.924) × 0.18 = 21.065,22
    VRR = 10.917 / 21.065,22 ≈ 0,5183 ≈ 0,52
    Gap = 21.065,22 - 10.917 = 10.148,22
"""

from __future__ import annotations

import logging
from decimal import Decimal

from gap_tributario.models import DadosVRR, ResultadoGap

logger = logging.getLogger(__name__)


class MotorVRR:
    """Motor de cálculo VRR (OCDE) para o Gap Tributário do ICMS."""

    def calcular(self, dados: DadosVRR) -> ResultadoGap:
        """Executa o cálculo VRR e retorna o resultado do gap.

        Args:
            dados: DadosVRR com todos os dados de entrada validados

        Returns:
            ResultadoGap com VRR, gap absoluto e percentual calculados

        Raises:
            ValueError: Se a base de cálculo VRR for zero ou negativa (VAB-Exp+Imp<=0)
        """
        base_calculo = dados.vab - dados.exportacoes_brl + dados.importacoes_brl

        if base_calculo <= Decimal("0"):
            raise ValueError(
                "Erro: base de cálculo VRR é zero (VAB-Exp+Imp=0). Verifique dados de entrada."
            )

        icms_potencial = base_calculo * dados.aliquota_padrao
        vrr = dados.icms_arrecadado / icms_potencial
        gap_absoluto = icms_potencial - dados.icms_arrecadado
        gap_percentual = (gap_absoluto / icms_potencial) * Decimal("100")

        logger.info("--- Cálculo VRR ---")
        logger.info("ICMS Potencial: %s milhões R$", icms_potencial)
        logger.info("VRR: %s", vrr)
        logger.info("Gap Absoluto: %s milhões R$", gap_absoluto)
        logger.info("Gap Percentual: %s%%", gap_percentual)

        return ResultadoGap(
            periodo=dados.periodo,
            icms_arrecadado=dados.icms_arrecadado,
            icms_potencial=icms_potencial,
            vrr=vrr,
            gap_absoluto=gap_absoluto,
            gap_percentual=gap_percentual,
            vab=dados.vab,
            exportacoes_brl=dados.exportacoes_brl,
            importacoes_brl=dados.importacoes_brl,
            aliquota_padrao=dados.aliquota_padrao,
            ptax_media=dados.ptax_media,
        )
