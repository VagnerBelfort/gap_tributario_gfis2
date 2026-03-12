"""Testes do motor de cálculo VRR.

Cobre os dados de referência MA 2022, alíquota 20%, edge cases e
propagação correta de todos os campos do ResultadoGap.

Dados de referência MA 2022:
    ICMS=10.917, VAB=124.859, Exp=29.754, Imp=21.924, Alíq=0.18
    ICMS Potencial = (124.859 - 29.754 + 21.924) × 0.18 = 21.065,22
    VRR = 10.917 / 21.065,22 ≈ 0,5183 ≈ 0,52
    Gap = 21.065,22 - 10.917 = 10.148,22
    Gap% = (10.148,22 / 21.065,22) × 100 ≈ 48,17%
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from gap_tributario.engine import MotorVRR
from gap_tributario.engine.vrr import MotorVRR as MotorVRRDireto
from gap_tributario.models import DadosVRR, PeriodoCalculo, ResultadoGap

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def motor() -> MotorVRR:
    """Instância do MotorVRR para uso nos testes."""
    return MotorVRR()


@pytest.fixture
def dados_ma_2022(periodo_2022_anual: PeriodoCalculo) -> DadosVRR:
    """Dados de referência MA 2022 conforme especificação ENCAT."""
    return DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("10917"),
        vab=Decimal("124859"),
        exportacoes_brl=Decimal("29754"),
        importacoes_brl=Decimal("21924"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )


@pytest.fixture
def dados_ma_2023(periodo_2023_anual: PeriodoCalculo) -> DadosVRR:
    """Dados simulados MA 2023 com alíquota 20%."""
    return DadosVRR(
        periodo=periodo_2023_anual,
        icms_arrecadado=Decimal("12000"),
        vab=Decimal("130000"),
        exportacoes_brl=Decimal("31000"),
        importacoes_brl=Decimal("23000"),
        aliquota_padrao=Decimal("0.20"),
        ptax_media=Decimal("5.10"),
    )


# ---------------------------------------------------------------------------
# Testes de importabilidade e instanciação
# ---------------------------------------------------------------------------


def test_motor_vrr_importable():
    """Verifica que o motor VRR é importável."""
    assert MotorVRR is not None


def test_motor_vrr_importable_direto():
    """Verifica que MotorVRR é importável diretamente do módulo vrr."""
    assert MotorVRRDireto is not None


def test_motor_vrr_instantiable():
    """Verifica que o motor VRR pode ser instanciado."""
    motor = MotorVRR()
    assert motor is not None


# ---------------------------------------------------------------------------
# Caso de referência MA 2022
# ---------------------------------------------------------------------------


def test_calcular_vrr_ma_2022_vrr_aproximado(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """VRR MA 2022 deve ser aproximadamente 0,5183 (tolerância ±0,001)."""
    resultado = motor.calcular(dados_ma_2022)
    vrr_esperado = Decimal("0.5183")
    tolerancia = Decimal("0.001")
    assert abs(resultado.vrr - vrr_esperado) <= tolerancia, (
        f"VRR esperado ≈ {vrr_esperado}, obtido {resultado.vrr}"
    )


def test_calcular_icms_potencial_ma_2022(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """ICMS Potencial MA 2022 deve ser ≈ 21.065,22."""
    resultado = motor.calcular(dados_ma_2022)
    # (124859 - 29754 + 21924) × 0.18 = 117029 × 0.18 = 21065.22
    potencial_esperado = Decimal("21065.22")
    tolerancia = Decimal("0.01")
    assert abs(resultado.icms_potencial - potencial_esperado) <= tolerancia, (
        f"Potencial esperado {potencial_esperado}, obtido {resultado.icms_potencial}"
    )


def test_calcular_gap_absoluto_ma_2022(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """Gap absoluto MA 2022 deve ser ≈ 10.148,22."""
    resultado = motor.calcular(dados_ma_2022)
    # 21065.22 - 10917 = 10148.22
    gap_esperado = Decimal("10148.22")
    tolerancia = Decimal("0.01")
    assert abs(resultado.gap_absoluto - gap_esperado) <= tolerancia, (
        f"Gap esperado {gap_esperado}, obtido {resultado.gap_absoluto}"
    )


def test_calcular_gap_percentual_ma_2022(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """Gap percentual MA 2022 deve ser ≈ 48,17%."""
    resultado = motor.calcular(dados_ma_2022)
    # (10148.22 / 21065.22) × 100 ≈ 48.17
    gap_pct_esperado = Decimal("48.17")
    tolerancia = Decimal("0.01")
    assert abs(resultado.gap_percentual - gap_pct_esperado) <= tolerancia, (
        f"Gap% esperado {gap_pct_esperado}, obtido {resultado.gap_percentual}"
    )


def test_calcular_retorna_resultado_gap(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """calcular() deve retornar instância de ResultadoGap."""
    resultado = motor.calcular(dados_ma_2022)
    assert isinstance(resultado, ResultadoGap)


# ---------------------------------------------------------------------------
# Caso com alíquota 20% (pós-2023)
# ---------------------------------------------------------------------------


def test_calcular_icms_potencial_aliquota_20(motor: MotorVRR, dados_ma_2023: DadosVRR):
    """Com alíquota 20%, o ICMS Potencial deve ser calculado corretamente."""
    resultado = motor.calcular(dados_ma_2023)
    # (130000 - 31000 + 23000) × 0.20 = 122000 × 0.20 = 24400
    potencial_esperado = Decimal("24400")
    assert resultado.icms_potencial == potencial_esperado, (
        f"Potencial esperado {potencial_esperado}, obtido {resultado.icms_potencial}"
    )


def test_calcular_vrr_aliquota_20(motor: MotorVRR, dados_ma_2023: DadosVRR):
    """Com alíquota 20%, VRR deve ser calculado corretamente."""
    resultado = motor.calcular(dados_ma_2023)
    # 12000 / 24400 ≈ 0.4918...
    vrr_esperado = Decimal("12000") / Decimal("24400")
    assert resultado.vrr == vrr_esperado


# ---------------------------------------------------------------------------
# Edge cases: base de cálculo zero ou negativa
# ---------------------------------------------------------------------------


def test_calcular_base_zero_levanta_value_error(
    motor: MotorVRR, periodo_2022_anual: PeriodoCalculo
):
    """Quando VAB - Exp + Imp = 0, deve levantar ValueError."""
    dados = DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("1000"),
        vab=Decimal("50000"),
        exportacoes_brl=Decimal("60000"),
        importacoes_brl=Decimal("10000"),  # 50000 - 60000 + 10000 = 0
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )
    with pytest.raises(ValueError, match="base de cálculo VRR é zero"):
        motor.calcular(dados)


def test_calcular_base_negativa_levanta_value_error(
    motor: MotorVRR, periodo_2022_anual: PeriodoCalculo
):
    """Quando VAB - Exp + Imp < 0, deve levantar ValueError."""
    dados = DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("1000"),
        vab=Decimal("10000"),
        exportacoes_brl=Decimal("20000"),
        importacoes_brl=Decimal("5000"),  # 10000 - 20000 + 5000 = -5000 < 0
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )
    with pytest.raises(ValueError, match="base de cálculo VRR é zero"):
        motor.calcular(dados)


def test_calcular_base_zero_mensagem_correta(
    motor: MotorVRR, periodo_2022_anual: PeriodoCalculo
):
    """Mensagem de erro deve conter o texto correto para base zero."""
    dados = DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("1000"),
        vab=Decimal("50000"),
        exportacoes_brl=Decimal("60000"),
        importacoes_brl=Decimal("10000"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )
    with pytest.raises(ValueError) as exc_info:
        motor.calcular(dados)
    assert "VAB-Exp+Imp=0" in str(exc_info.value)
    assert "Verifique dados de entrada" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Edge case: VRR > 1 (arrecadação supera potencial)
# ---------------------------------------------------------------------------


def test_calcular_vrr_maior_que_1_valido(
    motor: MotorVRR, periodo_2022_anual: PeriodoCalculo
):
    """VRR > 1 é válido (dados atípicos) — não deve levantar exceção."""
    dados = DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("30000"),  # superior ao potencial
        vab=Decimal("124859"),
        exportacoes_brl=Decimal("29754"),
        importacoes_brl=Decimal("21924"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )
    resultado = motor.calcular(dados)
    assert resultado.vrr > Decimal("1"), f"VRR deveria ser > 1, obtido: {resultado.vrr}"
    assert isinstance(resultado, ResultadoGap)


def test_calcular_vrr_maior_que_1_gap_negativo(
    motor: MotorVRR, periodo_2022_anual: PeriodoCalculo
):
    """Quando VRR > 1, gap_absoluto deve ser negativo."""
    dados = DadosVRR(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("30000"),
        vab=Decimal("124859"),
        exportacoes_brl=Decimal("29754"),
        importacoes_brl=Decimal("21924"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.22"),
    )
    resultado = motor.calcular(dados)
    assert resultado.gap_absoluto < Decimal("0")


# ---------------------------------------------------------------------------
# Verificação de propagação de campos
# ---------------------------------------------------------------------------


def test_calcular_propaga_periodo(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """O período deve ser propagado corretamente para ResultadoGap."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.periodo == dados_ma_2022.periodo
    assert resultado.periodo.ano == 2022
    assert resultado.periodo.trimestre is None


def test_calcular_propaga_icms_arrecadado(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """O ICMS arrecadado deve ser propagado sem alteração."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.icms_arrecadado == dados_ma_2022.icms_arrecadado


def test_calcular_propaga_vab(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """O VAB deve ser propagado para referência no relatório."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.vab == dados_ma_2022.vab


def test_calcular_propaga_exportacoes(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """As exportações devem ser propagadas para referência no relatório."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.exportacoes_brl == dados_ma_2022.exportacoes_brl


def test_calcular_propaga_importacoes(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """As importações devem ser propagadas para referência no relatório."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.importacoes_brl == dados_ma_2022.importacoes_brl


def test_calcular_propaga_aliquota(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """A alíquota padrão deve ser propagada para referência no relatório."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.aliquota_padrao == dados_ma_2022.aliquota_padrao


def test_calcular_propaga_ptax(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """A PTAX média deve ser propagada para referência no relatório."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.ptax_media == dados_ma_2022.ptax_media


def test_calcular_todos_campos_presentes(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """Todos os campos do ResultadoGap devem estar presentes e não nulos."""
    resultado = motor.calcular(dados_ma_2022)
    assert resultado.periodo is not None
    assert resultado.icms_arrecadado is not None
    assert resultado.icms_potencial is not None
    assert resultado.vrr is not None
    assert resultado.gap_absoluto is not None
    assert resultado.gap_percentual is not None
    assert resultado.vab is not None
    assert resultado.exportacoes_brl is not None
    assert resultado.importacoes_brl is not None
    assert resultado.aliquota_padrao is not None
    assert resultado.ptax_media is not None


# ---------------------------------------------------------------------------
# Verificação de consistência das fórmulas
# ---------------------------------------------------------------------------


def test_gap_percentual_consistente_com_gap_absoluto(
    motor: MotorVRR, dados_ma_2022: DadosVRR
):
    """gap_percentual = (gap_absoluto / icms_potencial) × 100."""
    resultado = motor.calcular(dados_ma_2022)
    gap_pct_calculado = (resultado.gap_absoluto / resultado.icms_potencial) * Decimal("100")
    assert resultado.gap_percentual == gap_pct_calculado


def test_vrr_consistente_com_potencial(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """vrr = icms_arrecadado / icms_potencial."""
    resultado = motor.calcular(dados_ma_2022)
    vrr_calculado = resultado.icms_arrecadado / resultado.icms_potencial
    assert resultado.vrr == vrr_calculado


def test_gap_absoluto_consistente_com_potencial(
    motor: MotorVRR, dados_ma_2022: DadosVRR
):
    """gap_absoluto = icms_potencial - icms_arrecadado."""
    resultado = motor.calcular(dados_ma_2022)
    gap_calculado = resultado.icms_potencial - resultado.icms_arrecadado
    assert resultado.gap_absoluto == gap_calculado


# ---------------------------------------------------------------------------
# Verificação de uso de Decimal (sem float)
# ---------------------------------------------------------------------------


def test_calcular_resultado_usa_decimal(motor: MotorVRR, dados_ma_2022: DadosVRR):
    """Todos os campos numéricos do ResultadoGap devem ser Decimal."""
    resultado = motor.calcular(dados_ma_2022)
    assert isinstance(resultado.icms_potencial, Decimal)
    assert isinstance(resultado.vrr, Decimal)
    assert isinstance(resultado.gap_absoluto, Decimal)
    assert isinstance(resultado.gap_percentual, Decimal)
