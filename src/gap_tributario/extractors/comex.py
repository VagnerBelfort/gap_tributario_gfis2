"""Extrator de dados de comércio exterior do MDIC ComEx.

Responsável por ler os CSVs do MDIC (exportações/importações)
e retornar os valores FOB em R$ milhões para o estado do Maranhão.

Formato dos arquivos:
- Separador: ;
- Encoding: latin-1
- Campos relevantes: CO_ANO, CO_MES, SG_UF_NCM, VL_FOB
- EXP_*.csv: exportações, IMP_*.csv: importações

Filtro: SG_UF_NCM == "MA" e período correspondente
Conversão: VL_FOB (USD) × ptax_media → BRL milhões
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple

import polars as pl

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)

# Mapeamento trimestre → meses correspondentes
_TRIMESTRE_MESES = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
}

# Fator de conversão: BRL (unidade) → R$ milhões
_FATOR_MILHOES = Decimal("1000000")


class ComexExtractor:
    """Extrator de dados de comércio exterior do MDIC ComEx."""

    def __init__(self, mdic_base_path: str) -> None:
        """Inicializa o extrator com o caminho base dos CSVs MDIC.

        Args:
            mdic_base_path: Caminho para o diretório dos CSVs MDIC
                            (ex: ./mdic_comex/dados/)
        """
        self.mdic_base_path = mdic_base_path

    def _listar_csvs(self, tipo: str, ano: int) -> List[Path]:
        """Lista os arquivos CSV de um tipo (EXP ou IMP) para um ano.

        Suporta os padrões: EXP_2022.csv, EXP_2022_NCM.csv, etc.

        Args:
            tipo: "EXP" para exportações ou "IMP" para importações
            ano: Ano a filtrar

        Returns:
            Lista de Paths dos CSVs encontrados
        """
        base = Path(self.mdic_base_path)
        csvs = (
            list(base.glob(f"{tipo}_{ano}*.csv"))
            + list(base.glob(f"{tipo}_{ano}*.CSV"))
        )
        return csvs

    def _ler_e_filtrar_csv(
        self,
        csvs: List[Path],
        tipo: str,
        periodo: PeriodoCalculo,
    ) -> Decimal:
        """Lê os CSVs, filtra por UF=MA e período, e soma VL_FOB.

        Args:
            csvs: Lista de arquivos CSV a processar
            tipo: "EXP" ou "IMP" (para mensagens de erro)
            periodo: Período de cálculo

        Returns:
            Decimal com a soma de VL_FOB em USD

        Raises:
            ExtractionError: Se ocorrer erro ao ler os CSVs ou se colunas
                             obrigatórias não forem encontradas
        """
        try:
            dfs = []
            for csv_path in csvs:
                df = pl.read_csv(
                    csv_path,
                    separator=";",
                    encoding="latin1",
                    infer_schema_length=1000,
                )
                dfs.append(df)
            df_total = pl.concat(dfs)
        except Exception as exc:
            raise ExtractionError(
                f"Erro ao ler CSVs MDIC {tipo} para {periodo.label}: {exc}. "
                f"Verifique se os arquivos em '{self.mdic_base_path}' são válidos."
            ) from exc

        # Validar colunas obrigatórias
        colunas_obrigatorias = ["SG_UF_NCM", "CO_ANO", "VL_FOB"]
        for coluna in colunas_obrigatorias:
            if coluna not in df_total.columns:
                raise ExtractionError(
                    f"Coluna '{coluna}' não encontrada nos CSVs MDIC {tipo}. "
                    f"Colunas disponíveis: {df_total.columns}"
                )

        if not periodo.is_anual and "CO_MES" not in df_total.columns:
            raise ExtractionError(
                f"Coluna 'CO_MES' não encontrada nos CSVs MDIC {tipo}. "
                f"Necessária para filtro trimestral."
            )

        # Filtrar por UF = MA
        df_ma = df_total.filter(pl.col("SG_UF_NCM") == "MA")

        # Filtrar por ano
        df_ma = df_ma.filter(pl.col("CO_ANO") == periodo.ano)

        # Filtrar por trimestre se necessário
        if not periodo.is_anual:
            meses = _TRIMESTRE_MESES[periodo.trimestre]
            df_ma = df_ma.filter(pl.col("CO_MES").is_in(meses))

        # Somar VL_FOB
        soma_fob = df_ma.select(pl.col("VL_FOB").fill_null(0).sum()).item()

        return Decimal(str(soma_fob))

    def extract(
        self,
        periodo: PeriodoCalculo,
        ptax_media: Decimal,
    ) -> Tuple[Decimal, Decimal]:
        """Extrai exportações e importações do Maranhão para o período.

        Lê os CSVs MDIC (EXP_*.csv e IMP_*.csv), filtra por UF=MA e período,
        soma VL_FOB em USD e converte para R$ milhões usando a PTAX média.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)
            ptax_media: Cotação média PTAX em R$/USD para conversão

        Returns:
            Tupla (exportacoes_brl, importacoes_brl) em R$ milhões

        Raises:
            ExtractionError: Se os CSVs MDIC não forem encontrados,
                             se ocorrer erro de leitura ou se colunas
                             obrigatórias estiverem ausentes
        """
        base = Path(self.mdic_base_path)

        if not base.exists():
            raise ExtractionError(
                f"Diretório de dados MDIC ComEx não encontrado: '{self.mdic_base_path}'. "
                f"Verifique se o caminho mdic_base_path em aliquotas.yaml está correto "
                f"e se os arquivos CSV estão disponíveis."
            )

        logger.info(
            "Lendo MDIC ComEx para exportações/importações MA %s: %s",
            periodo.label,
            str(base),
        )

        # --- Exportações ---
        csvs_exp = self._listar_csvs("EXP", periodo.ano)
        if not csvs_exp:
            raise ExtractionError(
                f"CSVs MDIC de exportações não encontrados para {periodo.ano} "
                f"em '{self.mdic_base_path}'. "
                f"Esperado: EXP_{periodo.ano}*.csv. "
                f"Baixe os dados em balanca.economia.gov.br."
            )

        soma_exp_usd = self._ler_e_filtrar_csv(csvs_exp, "EXP", periodo)

        # --- Importações ---
        csvs_imp = self._listar_csvs("IMP", periodo.ano)
        if not csvs_imp:
            raise ExtractionError(
                f"CSVs MDIC de importações não encontrados para {periodo.ano} "
                f"em '{self.mdic_base_path}'. "
                f"Esperado: IMP_{periodo.ano}*.csv. "
                f"Baixe os dados em balanca.economia.gov.br."
            )

        soma_imp_usd = self._ler_e_filtrar_csv(csvs_imp, "IMP", periodo)

        # Conversão USD → R$ milhões
        exportacoes_brl = (soma_exp_usd * ptax_media) / _FATOR_MILHOES
        importacoes_brl = (soma_imp_usd * ptax_media) / _FATOR_MILHOES

        logger.info(
            "MDIC ComEx MA %s: Exportações=%.3f M R$ (%.0f USD × %.4f PTAX), "
            "Importações=%.3f M R$ (%.0f USD × %.4f PTAX)",
            periodo.label,
            float(exportacoes_brl),
            float(soma_exp_usd),
            float(ptax_media),
            float(importacoes_brl),
            float(soma_imp_usd),
            float(ptax_media),
        )

        return exportacoes_brl, importacoes_brl
