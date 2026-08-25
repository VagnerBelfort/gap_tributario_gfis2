"""Comparação entre leituras de fontes diferentes para a mesma variável.

Existe para transformar a divergência entre Siscomex e MDIC em controle de
qualidade: o desvio entre as duas leituras tem magnitude e sinal esperados, e
sair desse corredor é sinal de problema no dado, não de escolha metodológica.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

# Corredor observado entre Siscomex (CIF, domicílio fiscal) e MDIC (FOB, UF do
# produto) de 2019 a 2025: +2,4% a +12,8%, mediana +6,8%. É a assinatura de CIF
# sobre FOB, líquida do trânsito por Itaqui, que opera em sentido oposto.
# Evidência ano a ano em docs/convergencia-importacoes.md §3.
CORREDOR_SISCOMEX_MDIC = (Decimal("2"), Decimal("13"))


@dataclass(frozen=True)
class ComparacaoLeituras:
    """Divergência entre a leitura vencedora da cascata e uma alternativa."""

    desvio_pct: Decimal  # (referência − alternativa) / alternativa, em %
    dentro_do_corredor: bool  # desvio compatível com CORREDOR_SISCOMEX_MDIC


def comparar_leituras(referencia: Decimal, alternativa: Decimal) -> ComparacaoLeituras:
    """Compara duas leituras da mesma variável.

    Args:
        referencia: Valor da fonte vencedora da cascata (R$ milhões)
        alternativa: Valor da fonte alternativa (R$ milhões)

    Returns:
        ComparacaoLeituras com o desvio percentual da referência sobre a
        alternativa e a checagem contra o corredor esperado.

    Raises:
        ValueError: Se a alternativa for zero (desvio indefinido).
    """
    if alternativa == 0:
        raise ValueError("Comparação exige alternativa diferente de zero")

    desvio = (referencia - alternativa) / alternativa * Decimal("100")
    piso, teto = CORREDOR_SISCOMEX_MDIC
    return ComparacaoLeituras(
        desvio_pct=desvio,
        dentro_do_corredor=piso <= desvio <= teto,
    )
