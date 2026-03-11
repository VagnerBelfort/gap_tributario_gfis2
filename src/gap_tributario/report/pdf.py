"""Gerador de relatório PDF com ReportLab.

Gera relatório PDF contendo:
- Tabela de resultados do Gap Tributário
- Seção de metodologia VRR/OCDE
- Dados de entrada utilizados no cálculo

Arquivo gerado: {saida}/gap_icms_{periodo}.pdf
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gap_tributario.models import AppConfig, DadosVRR, ResultadoGap

# TODO: Implementar na tarefa "Implementar gerador de relatório PDF"


class PDFReport:
    """Gerador de relatório PDF com ReportLab."""

    def gerar(
        self,
        resultado: "ResultadoGap",
        dados: "DadosVRR",
        config: "AppConfig",
        caminho_saida: Path,
    ) -> Path:
        """Gera o relatório PDF.

        Args:
            resultado: ResultadoGap com os valores calculados
            dados: DadosVRR com os dados de entrada
            config: AppConfig com a configuração da aplicação
            caminho_saida: Diretório de saída para o arquivo PDF

        Returns:
            Path para o arquivo PDF gerado

        Raises:
            IOError: Se não for possível escrever o arquivo de saída
        """
        raise NotImplementedError(
            "PDFReport não implementado. "
            "Implementar na tarefa 'Implementar gerador de relatório PDF'"
        )
