"""Motor de cálculo VRR para o Gap Tributário do ICMS-MA.

Implementa a fórmula VAT-VRR da OCDE:
    VRR = ICMS Arrecadado / [(VAB - Exportações + Importações) × Alíquota Padrão]
    Gap = ICMS Potencial - ICMS Arrecadado
    ICMS Potencial = (VAB - Exportações + Importações) × Alíquota Padrão
"""

from __future__ import annotations
