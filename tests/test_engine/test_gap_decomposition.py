"""Testes da decomposição do gap (policy vs compliance) — padrão RA-GAP/FMI.

decompor_gap separa o gap total em:
- policy_gap   = renúncia fiscal legal (AMF Tabela 7);
- compliance_gap = gap_total − policy_gap (evasão/inadimplência).

Golden 2022: Gap 10.148,22 = Policy 2.182,13 + Compliance 7.966,09.
"""

from __future__ import annotations

from decimal import Decimal

from gap_tributario.engine.gap_decomposition import decompor_gap
from gap_tributario.models import PeriodoCalculo, RenunciaFiscal, ResultadoGap


def _resultado(gap: Decimal) -> ResultadoGap:
    """ResultadoGap mínimo com o gap absoluto desejado (demais campos irrelevantes)."""
    return ResultadoGap(
        periodo=PeriodoCalculo(ano=2022),
        icms_arrecadado=Decimal("10917"),
        icms_potencial=Decimal("10917") + gap,
        vrr=Decimal("0.5183"),
        gap_absoluto=gap,
        gap_percentual=Decimal("48.17"),
        vab=Decimal("124859"),
        exportacoes_brl=Decimal("29754"),
        importacoes_brl=Decimal("21924"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )


def _renuncia(total: str) -> RenunciaFiscal:
    return RenunciaFiscal(ano=2022, total=Decimal(total), vintage="LDO-2022")


def test_decompoe_golden_2022() -> None:
    """Gap 10.148,22 decompõe em Policy 2.182,13 + Compliance 7.966,09."""
    dec = decompor_gap(_resultado(Decimal("10148.22")), _renuncia("2182.13"))

    assert dec.policy_gap == Decimal("2182.13")
    assert dec.compliance_gap == Decimal("7966.09")
    assert dec.gap_total == Decimal("10148.22")
    assert not dec.compliance_negativo


def test_percentuais_somam_cem_por_cento() -> None:
    """policy_pct + compliance_pct = 100% do gap total."""
    dec = decompor_gap(_resultado(Decimal("10000")), _renuncia("2500"))
    assert dec.policy_pct == Decimal("25")
    assert dec.compliance_pct == Decimal("75")
    assert dec.policy_pct + dec.compliance_pct == Decimal("100")


def test_renuncia_maior_que_gap_marca_compliance_negativo() -> None:
    """Renúncia > gap → compliance_gap negativo e flag levantada."""
    dec = decompor_gap(_resultado(Decimal("2000")), _renuncia("2182.13"))
    assert dec.compliance_gap == Decimal("-182.13")
    assert dec.compliance_negativo


def test_gap_total_zero_nao_divide_por_zero() -> None:
    """Gap total zero → percentuais zerados (sem divisão por zero)."""
    dec = decompor_gap(_resultado(Decimal("0")), _renuncia("0"))
    assert dec.policy_pct == Decimal("0")
    assert dec.compliance_pct == Decimal("0")
