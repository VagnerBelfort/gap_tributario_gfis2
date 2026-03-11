"""Testes do motor de cálculo VRR.

Scaffolding — verificam importabilidade.
Testes completos com dados de referência MA 2022 serão implementados
na tarefa do motor de cálculo VRR.

Dados de referência MA 2022:
    ICMS=10.917, VAB=124.859, Exp=29.754, Imp=21.924, Alíq=0.18
    ICMS Potencial = (124.859 - 29.754 + 21.924) × 0.18 = 21.065,22
    VRR = 10.917 / 21.065,22 ≈ 0,5183 ≈ 0,52
    Gap = 21.065,22 - 10.917 = 10.148,22
"""

from __future__ import annotations

from gap_tributario.engine.vrr import MotorVRR


def test_motor_vrr_importable():
    """Verifica que o motor VRR é importável."""
    assert MotorVRR is not None


def test_motor_vrr_instantiable():
    """Verifica que o motor VRR pode ser instanciado."""
    motor = MotorVRR()
    assert motor is not None
