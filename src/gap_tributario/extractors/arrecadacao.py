"""Extrator de dados de arrecadação ICMS do GFIS2 (Parquet ouro).

Responsável por ler os arquivos Parquet da camada ouro (g_arrecadacao)
e retornar o ICMS arrecadado para o período especificado.

Campos lidos (schema real — diverge do TechSpec):
- per_aaaa: Int32 — Ano do período  (TechSpec documenta per_nro_ano)
- per_nro_trimestre: Int32 — Trimestre (1-4)
- val_icms_normal: Float64 — ICMS normal arrecadado
- val_icms_imp: Float64 — ICMS importação arrecadado
- val_icms_st_sda: Float64 — ICMS substituição tributária saída  (TechSpec documenta val_icms_st)

Unidade no Parquet: R$ (unidade). Conversão para milhões feita na extração (÷ 1.000.000).

Agregação: val_icms_normal + val_icms_imp + val_icms_st_sda → icms_arrecadado (R$ milhões)
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import polars as pl

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)

# Colunas ICMS a somar para obter o ICMS arrecadado total
_COLUNAS_ICMS = ["val_icms_normal", "val_icms_imp", "val_icms_st_sda"]

# Fator de conversão: R$ unitário → R$ milhões
_FATOR_MILHOES = Decimal("1000000")


class ArrecadacaoExtractor:
    """Extrator de dados de arrecadação ICMS do GFIS2 Parquet."""

    def __init__(self, parquet_base_path: str) -> None:
        """Inicializa o extrator com o caminho base dos Parquets.

        Args:
            parquet_base_path: Caminho para o diretório dos arquivos Parquet ouro
                               (ex: bases/bases_arrecadacao_cruzamento/gfis2_ouro/g_arrecadacao/)
        """
        self.parquet_base_path = parquet_base_path

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai o ICMS arrecadado para o período via leitura lazy de Parquet.

        Lê os arquivos Parquet da camada ouro usando scan_parquet() (lazy evaluation)
        para suportar o arquivo de ~1.6GB sem carregar tudo em memória. Filtra por
        ano (per_aaaa) e, quando trimestral, também por per_nro_trimestre. Soma as
        colunas val_icms_normal, val_icms_imp e val_icms_st_sda e converte o resultado
        de R$ unitário para R$ milhões.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com o ICMS arrecadado em milhões de R$

        Raises:
            ExtractionError: Se o caminho Parquet não existir, estiver corrompido,
                             ou não houver dados para o período solicitado
        """
        caminho = Path(self.parquet_base_path)

        if not caminho.exists():
            raise ExtractionError(
                f"Diretório de dados GFIS2 não encontrado: '{self.parquet_base_path}'. "
                f"Verifique se o caminho parquet_base_path em aliquotas.yaml está correto "
                f"e se os arquivos Parquet estão disponíveis."
            )

        padrao_parquet = str(caminho / "*.parquet")

        logger.info(
            "Lendo GFIS2 Parquet para ICMS arrecadado %s: %s",
            periodo.label,
            padrao_parquet,
        )

        try:
            lf = pl.scan_parquet(padrao_parquet)

            # Filtro por ano
            lf = lf.filter(pl.col("per_aaaa") == periodo.ano)

            # Filtro adicional por trimestre (apenas quando não é período anual)
            if not periodo.is_anual:
                lf = lf.filter(pl.col("per_nro_trimestre") == periodo.trimestre)

            # Verifica se há registros para o período antes de somar
            contagem = lf.select(pl.len()).collect().item()

        except pl.exceptions.PolarsError as exc:
            raise ExtractionError(
                f"Erro ao ler arquivos Parquet GFIS2 em '{self.parquet_base_path}': {exc}. "
                f"Verifique se os arquivos não estão corrompidos."
            ) from exc

        if contagem == 0:
            raise ExtractionError(
                f"Nenhum dado encontrado em '{self.parquet_base_path}' "
                f"para o período {periodo.label}. "
                f"Verifique se o Parquet contém dados para o ano {periodo.ano}."
            )

        try:
            # Soma das colunas ICMS com tratamento de nulos (null → 0)
            lf_filtrado = pl.scan_parquet(padrao_parquet).filter(
                pl.col("per_aaaa") == periodo.ano
            )
            if not periodo.is_anual:
                lf_filtrado = lf_filtrado.filter(
                    pl.col("per_nro_trimestre") == periodo.trimestre
                )

            resultado = lf_filtrado.select(
                [
                    pl.col(col).fill_null(0.0).sum().alias(col)
                    for col in _COLUNAS_ICMS
                ]
            ).collect()

        except pl.exceptions.PolarsError as exc:
            raise ExtractionError(
                f"Erro ao ler arquivos Parquet GFIS2 em '{self.parquet_base_path}': {exc}. "
                f"Verifique se os arquivos não estão corrompidos."
            ) from exc

        # Extrai os valores e soma
        row = resultado.row(0)
        soma_reais = sum(row)  # soma em R$ unitário

        # Converte para Decimal e divide por 1.000.000 para obter R$ milhões
        icms_milhoes = Decimal(str(soma_reais)) / _FATOR_MILHOES

        logger.info(
            "ICMS arrecadado %s: R$ %.2f milhões (normal=%.2f M, imp=%.2f M, st_sda=%.2f M)",
            periodo.label,
            float(icms_milhoes),
            row[0] / 1_000_000,
            row[1] / 1_000_000,
            row[2] / 1_000_000,
        )

        return icms_milhoes
