"""Calculadora de Gap Tributário do ICMS-MA.

Implementa a metodologia VAT-VRR da OCDE adaptada para estados brasileiros.
Fórmula central:
    VRR = ICMS Arrecadado / [(VAB - Exportações + Importações) × Alíquota Padrão]
    Gap = ICMS Potencial - ICMS Arrecadado
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "SEFAZ-MA"
__description__ = "Calculadora de Gap Tributário do ICMS-MA (VRR/OCDE)"

__all__ = ["__version__", "__author__", "__description__"]
