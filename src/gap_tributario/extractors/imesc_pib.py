"""Extrator do Valor Adicionado Bruto (VAB) via Relatório PIB Trimestral do IMESC.

Fonte primária de VAB da cascata. Lê os valores correntes (nominais) transcritos
da Tabela 15 do *Relatório Especializado do PIB Trimestral do Maranhão* (IMESC /
Governo do MA) e retorna o VAB do Maranhão em milhões de R$.

Diferente do IBGEExtractor (que estima VAB a partir do PIB com o fator 0,8932 e
divide por 4 no trimestre), aqui o VAB é um dado direto:

- Período anual: VAB = soma dos 4 trimestres do ano.
- Período trimestral: VAB = valor do trimestre, direto (sem rateio).

Cobertura: 2021–2025. Anos fora da cobertura levantam ExtractionError, deixando a
cascata cair para a próxima fonte (IBGE SIDRA).
"""

from __future__ import annotations

import csv
import logging
from decimal import Decimal
from pathlib import Path

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)

# Asset versionado: Tabela 15 (valores correntes) transcrita do PDF do IMESC.
_DADOS_PADRAO = Path(__file__).resolve().parent.parent / "data" / "imesc_pib_trimestral_ma.csv"


class ImescPibExtractor:
    """Extrator do VAB do Maranhão a partir do Relatório PIB Trimestral (IMESC)."""

    def __init__(self, data_path: Path | None = None) -> None:
        """Inicializa o extrator.

        Args:
            data_path: Caminho para o CSV com os valores correntes da Tabela 15.
                       Default: asset versionado empacotado com o código.
        """
        self.data_path = data_path or _DADOS_PADRAO

    def _carregar_vab_por_trimestre(self) -> dict[tuple[int, int], Decimal]:
        """Carrega o VAB (R$ milhões) indexado por (ano, trimestre)."""
        vab: dict[tuple[int, int], Decimal] = {}
        with self.data_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(filter(lambda ln: not ln.startswith("#"), f))
            for row in reader:
                chave = (int(row["ano"]), int(row["trimestre"]))
                vab[chave] = Decimal(row["vab"])
        return vab

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai o VAB do Maranhão para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com o VAB em milhões R$.

        Raises:
            ExtractionError: Se o período estiver fora da cobertura do IMESC.
        """
        vab_por_tri = self._carregar_vab_por_trimestre()

        if periodo.is_anual:
            trimestres = [vab_por_tri[(periodo.ano, t)] for t in (1, 2, 3, 4) if (periodo.ano, t) in vab_por_tri]
            if len(trimestres) != 4:
                raise ExtractionError(
                    f"IMESC não cobre o ano {periodo.ano} (esperados 4 trimestres, "
                    f"encontrados {len(trimestres)}). Caindo para a próxima fonte da cascata."
                )
            vab = sum(trimestres, Decimal("0"))
        else:
            chave = (periodo.ano, periodo.trimestre)  # type: ignore[arg-type]
            if chave not in vab_por_tri:
                raise ExtractionError(
                    f"IMESC não cobre o período {periodo.label}. "
                    f"Caindo para a próxima fonte da cascata."
                )
            vab = vab_por_tri[chave]

        logger.info("VAB IMESC %s: R$ %s milhões", periodo.label, vab)
        return vab
