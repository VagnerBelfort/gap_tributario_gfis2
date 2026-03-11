"""Extrator de dados de arrecadação ICMS do GFIS2 (Parquet ouro).

Responsável por ler os arquivos Parquet da camada ouro (g_arrecadacao)
e retornar o ICMS arrecadado para o período especificado.

Campos lidos:
- per_nro_ano: Int32 — Ano do período
- per_nro_trimestre: Int32 — Trimestre (1-4)
- val_icms_normal: Float64 — ICMS normal arrecadado
- val_icms_imp: Float64 — ICMS importação arrecadado
- val_icms_st: Float64 — ICMS substituição tributária

Agregação: val_icms_normal + val_icms_imp + val_icms_st → icms_arrecadado
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar extrator GFIS2 Parquet ICMS arrecadado"


class ArrecadacaoExtractor:
    """Extrator de dados de arrecadação ICMS do GFIS2 Parquet."""

    def __init__(self, parquet_base_path: str) -> None:
        """Inicializa o extrator com o caminho base dos Parquets.

        Args:
            parquet_base_path: Caminho para o diretório dos arquivos Parquet ouro
        """
        self.parquet_base_path = parquet_base_path

    def extract(self, periodo: object) -> object:
        """Extrai dados de arrecadação ICMS para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            DataFrame Polars com icms_arrecadado em milhões R$

        Raises:
            ExtractionError: Se os arquivos Parquet não forem encontrados
        """
        raise NotImplementedError(
            "ArrecadacaoExtractor não implementado. "
            "Implementar na tarefa 'Implementar extrator GFIS2 Parquet ICMS arrecadado'"
        )
