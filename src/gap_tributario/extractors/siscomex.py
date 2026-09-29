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
from typing import Dict, Union

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

# Primeiro ano com cobertura confiável no Siscomex. Antes disso a base tem
# poucas DIs (205 em 2011, 1.808 em 2012, contra ~3.000/ano de 2013 em diante),
# e somar o que existe devolveria um total muito abaixo do real com aparência
# de número válido. Abaixo deste piso preferimos falhar e deixar a cascata cair
# para o MDIC, que ao menos cobre o período inteiro.
_ANO_COBERTURA_CONFIAVEL = 2013

# Declaração de origem de cada linha do snapshot. A DUIMP (Portal Único)
# substitui a DI a partir de nov/2025; snapshot anterior não tem a coluna.
_COLUNA_FONTE = "fonte"
_FONTE_DI = "DI"


class SiscomexSnapshotExtractor:
    """Extrai importações do MA (CIF, R$ milhões) do snapshot do Siscomex."""

    def __init__(
        self,
        snapshot_path: Union[str, Path],
        *,
        incluir_nao_identificado: bool = False,
        ano_minimo: int = _ANO_COBERTURA_CONFIAVEL,
    ) -> None:
        """Inicializa o extrator.

        Args:
            snapshot_path: Caminho do snapshot agregado (.csv ou .parquet).
            incluir_nao_identificado: Soma também as DIs cujo importador não
                pôde ser atribuído a uma UF (`NI`). Padrão False — atribuir ao
                MA o que não foi comprovado infla a base. Use True para obter
                o teto do intervalo numa análise de sensibilidade.
            ano_minimo: Primeiro ano aceito. Anos anteriores levantam
                ExtractionError em vez de devolver total subestimado.
        """
        self.snapshot_path = Path(snapshot_path)
        self.incluir_nao_identificado = incluir_nao_identificado
        self.ano_minimo = ano_minimo

    def _scan(self) -> "pl.LazyFrame":
        """Lê o snapshot conforme a extensão: .parquet ou CSV com ';'."""
        if self.snapshot_path.suffix.lower() == ".parquet":
            return pl.scan_parquet(self.snapshot_path)
        return pl.scan_csv(self.snapshot_path, separator=";")

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai as importações do importador maranhense no período.

        Soma DI e DUIMP: cada importação é declarada em um dos dois documentos.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual).

        Returns:
            Decimal com o valor aduaneiro (CIF) em milhões de R$.
        """
        total = self._cif_por_fonte(periodo)["cif"].sum()
        return (Decimal(str(total)) / _FATOR_MILHOES).quantize(Decimal("0.01"))

    def extract_por_fonte(self, periodo: PeriodoCalculo) -> Dict[str, Decimal]:
        """Importações do MA no período separadas por declaração (DI, DUIMP).

        Snapshot anterior à DUIMP, sem a coluna `fonte`, é todo DI.

        Returns:
            Dicionário fonte → CIF em milhões de R$. Só aparecem as fontes com
            linhas no período.
        """
        return {
            linha["fonte"]: (Decimal(str(linha["cif"])) / _FATOR_MILHOES).quantize(
                Decimal("0.01")
            )
            for linha in self._cif_por_fonte(periodo).iter_rows(named=True)
        }

    def _cif_por_fonte(self, periodo: PeriodoCalculo) -> "pl.DataFrame":
        """CIF em R$ do importador MA no período, uma linha por fonte."""
        if periodo.ano < self.ano_minimo:
            raise ExtractionError(
                f"Período {periodo.label} está fora da cobertura confiável do "
                f"Siscomex (a partir de {self.ano_minimo}). Caindo para a próxima "
                f"fonte da cascata (MDIC ComEx)."
            )

        if not self.snapshot_path.exists():
            raise ExtractionError(
                f"Snapshot do Siscomex não encontrado: '{self.snapshot_path}'. "
                f"Gere-o no cluster da SEFAZ com scripts/snapshot_siscomex.py. "
                f"Caindo para a próxima fonte da cascata (MDIC ComEx)."
            )

        ufs = [_UF_MA, _UF_NAO_IDENTIFICADA] if self.incluir_nao_identificado else [_UF_MA]

        # Snapshot corrompido, truncado ou gerado por uma versão antiga do job
        # levanta erro cru do polars. Sem traduzir para ExtractionError, o cli
        # não captura e a cascata nunca cai para o MDIC.
        try:
            lf = self._scan()
            if _COLUNA_FONTE not in lf.collect_schema().names():
                lf = lf.with_columns(pl.lit(_FONTE_DI).alias(_COLUNA_FONTE))
            lf = lf.filter(
                (pl.col("ano") == periodo.ano) & (pl.col("uf_importador").is_in(ufs))
            )
            if not periodo.is_anual:
                lf = lf.filter(pl.col("trimestre") == periodo.trimestre)

            por_fonte = (
                lf.group_by(_COLUNA_FONTE)
                .agg(pl.col("cif_brl").sum().fill_null(0).alias("cif"))
                .sort(_COLUNA_FONTE)
                .collect()
            )
        except pl.exceptions.PolarsError as exc:
            raise ExtractionError(
                f"Snapshot do Siscomex em formato inesperado ('{self.snapshot_path}'): "
                f"{exc}. Regere-o com scripts/snapshot_siscomex.py. "
                f"Caindo para a próxima fonte da cascata (MDIC ComEx)."
            ) from exc

        if por_fonte.height == 0:
            raise ExtractionError(
                f"Snapshot do Siscomex não tem importações do {_UF_MA} para "
                f"{periodo.label} (ano {periodo.ano}). A cobertura confiável começa "
                f"em 2013. Caindo para a próxima fonte da cascata (MDIC ComEx)."
            )
        return por_fonte
