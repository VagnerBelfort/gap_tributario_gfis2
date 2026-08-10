"""Extrator de importações a partir do snapshot agregado do Siscomex.

Fonte nº1 da cascata de importações. Substitui o filtro por capítulo NCM sobre
o MDIC, que atribuía ao Maranhão tudo que desembaraçava em Itaqui.

O snapshot é materializado no cluster da SEFAZ por `scripts/snapshot_siscomex.py`
(PySpark → Oracle C3 `APL_SISCOMEX`), porque o Oracle só é alcançável de dentro
da rede. O CSV exportado é AGREGADO — sem CNPJ, sem nome de importador, sem
nível de declaração — com uma linha por:

    ano × trimestre × capítulo NCM × UF de despacho × UF do importador

A `UF do importador` é o domicílio fiscal declarado na DI. É ela que separa a
importação maranhense da carga em trânsito por Itaqui — distinção que o
`SG_UF_NCM` do MDIC não faz, por ser local de desembaraço.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import Union

import polars as pl

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)

# UF de domicílio fiscal do projeto e fator R$ unitário → R$ milhões.
_UF_MA = "MA"
_FATOR_MILHOES = Decimal("1000000")

# Marca do job Spark para DI cujo importador não pôde ser atribuído a uma UF:
# UF ausente na declaração e CNPJ não resolvido no cadastro de contribuintes.
_UF_NAO_IDENTIFICADA = "NI"


class SiscomexSnapshotExtractor:
    """Extrai importações do MA (CIF, R$ milhões) do snapshot do Siscomex."""

    def __init__(
        self,
        snapshot_path: Union[str, Path],
        *,
        incluir_nao_identificado: bool = False,
    ) -> None:
        """Inicializa o extrator.

        Args:
            snapshot_path: Caminho do snapshot agregado (.csv ou .parquet).
            incluir_nao_identificado: Soma também as DIs cujo importador não
                pôde ser atribuído a uma UF (`NI`). Padrão False — atribuir ao
                MA o que não foi comprovado infla a base. Use True para obter
                o teto do intervalo numa análise de sensibilidade.
        """
        self.snapshot_path = Path(snapshot_path)
        self.incluir_nao_identificado = incluir_nao_identificado

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai as importações do importador maranhense no período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual).

        Returns:
            Decimal com o valor aduaneiro (CIF) em milhões de R$.
        """
        if not self.snapshot_path.exists():
            raise ExtractionError(
                f"Snapshot do Siscomex não encontrado: '{self.snapshot_path}'. "
                f"Gere-o no cluster da SEFAZ com scripts/snapshot_siscomex.py. "
                f"Caindo para a próxima fonte da cascata (MDIC ComEx)."
            )

        ufs = [_UF_MA, _UF_NAO_IDENTIFICADA] if self.incluir_nao_identificado else [_UF_MA]
        lf = pl.scan_csv(self.snapshot_path, separator=";").filter(
            (pl.col("ano") == periodo.ano) & (pl.col("uf_importador").is_in(ufs))
        )
        if not periodo.is_anual:
            lf = lf.filter(pl.col("trimestre") == periodo.trimestre)

        resumo = lf.select(
            pl.col("cif_brl").sum().alias("cif"), pl.len().alias("linhas")
        ).collect()
        if resumo["linhas"].item() == 0:
            raise ExtractionError(
                f"Snapshot do Siscomex não tem importações do {_UF_MA} para "
                f"{periodo.label} (ano {periodo.ano}). A cobertura confiável começa "
                f"em 2013. Caindo para a próxima fonte da cascata (MDIC ComEx)."
            )

        total = resumo["cif"].item()
        return (Decimal(str(total)) / _FATOR_MILHOES).quantize(Decimal("0.01"))
