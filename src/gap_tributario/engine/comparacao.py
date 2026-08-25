"""Comparação entre leituras de fontes diferentes para a mesma variável.

Existe para transformar a divergência entre Siscomex e MDIC em controle de
qualidade: o desvio entre as duas leituras tem magnitude e sinal esperados, e
sair desse corredor é sinal de problema no dado, não de escolha metodológica.
"""

from __future__ import annotations

from decimal import Decimal

from gap_tributario.models import ComparacaoLeituras

# Faixa medida entre Siscomex (CIF, domicílio fiscal) e MDIC (FOB, UF do
# produto) nos totais ANUAIS de 2019 a 2025. É a assinatura de CIF sobre FOB,
# líquida do trânsito por Itaqui, que opera em sentido oposto. Evidência ano a
# ano em docs/convergencia-importacoes.md §3.
FAIXA_OBSERVADA = (Decimal("2.4"), Decimal("12.8"))
MEDIANA_OBSERVADA = Decimal("6.8")

# Tolerância do controle, deliberadamente mais larga que a faixa observada: o
# extremo inferior medido (2022) é +2,368%, que só vira "+2,4%" na exibição.
# Um controle colado no valor exibido reprovaria justamente o ano que o
# originou.
CORREDOR_CONTROLE = (Decimal("2"), Decimal("13"))


def _formatar(valor: Decimal) -> str:
    """Formata um percentual da faixa com sinal e vírgula: +2,4%."""
    return f"{valor:+.1f}%".replace(".", ",")


def descrever_faixa_observada() -> str:
    """Descreve a faixa medida, para os textos de log e de relatório.

    Fonte única: quem alterar as constantes altera todos os textos.
    """
    piso, teto = FAIXA_OBSERVADA
    return f"{_formatar(piso)} a {_formatar(teto)} (mediana {_formatar(MEDIANA_OBSERVADA)})"


def descrever_corredor_controle() -> str:
    """Descreve a tolerância aplicada pelo controle."""
    piso, teto = CORREDOR_CONTROLE
    return f"{_formatar(piso)} a {_formatar(teto)}"


def comparar_leituras(
    referencia: Decimal,
    alternativa: Decimal,
    aplicar_corredor: bool = True,
) -> ComparacaoLeituras:
    """Compara duas leituras da mesma variável.

    Args:
        referencia: Valor da fonte vencedora da cascata (R$ milhões)
        alternativa: Valor da fonte alternativa (R$ milhões)
        aplicar_corredor: Se o corredor de controle vale para o período. Falso
            para períodos infra-anuais, em que a faixa não foi medida.

    Returns:
        ComparacaoLeituras com o desvio percentual da referência sobre a
        alternativa e, quando aplicável, a checagem contra o corredor.

    Raises:
        ValueError: Se a alternativa for zero (desvio indefinido).
    """
    if alternativa == 0:
        raise ValueError("Comparação exige alternativa diferente de zero")

    desvio = (referencia - alternativa) / alternativa * Decimal("100")
    if not aplicar_corredor:
        return ComparacaoLeituras(desvio_pct=desvio)

    piso, teto = CORREDOR_CONTROLE
    return ComparacaoLeituras(
        desvio_pct=desvio,
        dentro_do_corredor=piso <= desvio <= teto,
    )
