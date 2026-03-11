"""Extrator de dados de comércio exterior do MDIC ComEx.

Responsável por ler os CSVs do MDIC (exportações/importações)
e retornar os valores FOB em R$ para o estado do Maranhão.

Formato dos arquivos:
- Separador: ;
- Encoding: latin-1
- Campos relevantes: CO_ANO, CO_MES, SG_UF_NCM, VL_FOB
- EXP_*.csv: exportações, IMP_*.csv: importações

Filtro: SG_UF_NCM == "MA" e período correspondente
Conversão: VL_FOB (USD) × ptax_media → BRL
"""

from __future__ import annotations

# TODO: Implementar na tarefa "Implementar extrator MDIC ComEx exportações e importações"


class ComexExtractor:
    """Extrator de dados de comércio exterior do MDIC ComEx."""

    def __init__(self, mdic_base_path: str) -> None:
        """Inicializa o extrator com o caminho base dos CSVs MDIC.

        Args:
            mdic_base_path: Caminho para o diretório dos CSVs MDIC
        """
        self.mdic_base_path = mdic_base_path

    def extract(self, periodo: object, ptax_media: object) -> object:
        """Extrai dados de exportações e importações para o período.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)
            ptax_media: Cotação média PTAX (Decimal) para conversão USD→BRL

        Returns:
            DataFrame Polars com exportacoes_brl e importacoes_brl em milhões R$

        Raises:
            ExtractionError: Se os CSVs MDIC não forem encontrados
        """
        raise NotImplementedError(
            "ComexExtractor não implementado. "
            "Implementar na tarefa 'Implementar extrator MDIC ComEx exportações e importações'"
        )
