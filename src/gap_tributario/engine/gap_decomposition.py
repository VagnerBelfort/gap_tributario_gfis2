"""Decomposição do gap tributário em policy gap e compliance gap (RA-GAP/FMI).

Estágio aditivo entre Calculate e Report — **não** altera o ``MotorVRR``. Recebe
o ``ResultadoGap`` já calculado e a renúncia fiscal do ano (AMF Tabela 7) e
separa o gap total em:

- ``policy_gap``     = renúncia fiscal legal (gasto tributário);
- ``compliance_gap`` = ``gap_total − policy_gap`` (evasão/inadimplência).

Quando a renúncia excede o gap total, ``compliance_gap`` fica negativo e a flag
``compliance_negativo`` é levantada para sinalização no relatório.
"""

from __future__ import annotations

from decimal import Decimal

from gap_tributario.models import DecomposicaoGap, RenunciaFiscal, ResultadoGap

_CEM = Decimal("100")


def decompor_gap(resultado: ResultadoGap, renuncia: RenunciaFiscal) -> DecomposicaoGap:
    """Decompõe o gap total em policy gap (renúncia) e compliance gap (evasão).

    Args:
        resultado: ResultadoGap já calculado pelo MotorVRR (gap em R$ milhões).
        renuncia: RenunciaFiscal do ano (R$ milhões), tipicamente da AMF Tabela 7.

    Returns:
        DecomposicaoGap com os componentes e seus percentuais sobre o gap total.
    """
    gap_total = resultado.gap_absoluto
    policy_gap = renuncia.total
    compliance_gap = gap_total - policy_gap

    if gap_total != Decimal("0"):
        policy_pct = (policy_gap / gap_total) * _CEM
        compliance_pct = (compliance_gap / gap_total) * _CEM
    else:
        policy_pct = Decimal("0")
        compliance_pct = Decimal("0")

    return DecomposicaoGap(
        gap_total=gap_total,
        policy_gap=policy_gap,
        compliance_gap=compliance_gap,
        policy_pct=policy_pct,
        compliance_pct=compliance_pct,
        renuncia=renuncia,
        compliance_negativo=compliance_gap < Decimal("0"),
    )
