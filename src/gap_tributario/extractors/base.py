"""Interface base (Protocol) para todos os extratores de dados."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from gap_tributario.models import PeriodoCalculo


class ExtractionError(Exception):
    """Erro levantado quando uma fonte de dados está indisponível."""

    pass


class Extractor:
    """Interface base para todos os extratores de dados.

    Todos os extratores devem implementar o método `extract`.
    """

    def extract(self, periodo: "PeriodoCalculo") -> "pl.DataFrame":
        """Extrai dados para o período especificado.

        Args:
            periodo: Período de cálculo (trimestral ou anual)

        Returns:
            DataFrame Polars com os dados extraídos

        Raises:
            ExtractionError: Se a fonte de dados estiver indisponível
        """
        raise NotImplementedError("Subclasses devem implementar extract()")
