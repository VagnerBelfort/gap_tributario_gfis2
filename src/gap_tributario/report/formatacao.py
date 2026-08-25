"""Formatação compartilhada entre os backends de relatório (PDF e Excel).

Os dois renderizam o mesmo conteúdo em mídias diferentes; o texto que descreve
os números precisa nascer de um lugar só, senão PDF e Excel divergem em
silêncio.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from gap_tributario.models import ComparacaoLeituras


def formatar_desvio(valor: Decimal) -> str:
    """Formata o desvio entre fontes com sinal explícito: +2,4% / -76,1%."""
    arredondado = valor.quantize(Decimal("0.1"))
    return f"{arredondado:+.1f}%".replace(".", ",")


def sufixo_desvio(
    comparacao: "Optional[ComparacaoLeituras]", alerta_html: bool = False
) -> str:
    """Texto que acompanha uma leitura alternativa, com o desvio e o alerta.

    Args:
        comparacao: Comparação da leitura, ou None quando não houver
        alerta_html: Se o alerta de corredor deve vir em negrito (PDF)

    Returns:
        Sufixo pronto para concatenar, ou string vazia.
    """
    if comparacao is None:
        return ""

    texto = f" (desvio da fonte adotada: {formatar_desvio(comparacao.desvio_pct)}"
    if comparacao.dentro_do_corredor is False:
        alerta = "fora do corredor esperado"
        texto += f", <b>{alerta}</b>" if alerta_html else f", {alerta}"
    texto += ")"
    return texto
