"""Gerador de relatório Excel com XlsxWriter.

Gera planilha Excel contendo:
- Tabela de resultados do Gap Tributário
- Campos do contrato ResultadoGap em células nomeadas
- Seção de metodologia VRR/OCDE

Arquivo gerado: {saida}/gap_icms_{periodo}.xlsx
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gap_tributario.models import AppConfig, DadosVRR, ResultadoGap

# TODO: Implementar na tarefa "Implementar gerador de relatório Excel"


class ExcelReport:
    """Gerador de relatório Excel com XlsxWriter."""

    def gerar(
        self,
        resultado: "ResultadoGap",
        dados: "DadosVRR",
        config: "AppConfig",
        caminho_saida: Path,
    ) -> Path:
        """Gera o relatório Excel.

        Args:
            resultado: ResultadoGap com os valores calculados
            dados: DadosVRR com os dados de entrada
            config: AppConfig com a configuração da aplicação
            caminho_saida: Diretório de saída para o arquivo Excel

        Returns:
            Path para o arquivo Excel gerado

        Raises:
            IOError: Se não for possível escrever o arquivo de saída
        """
        raise NotImplementedError(
            "ExcelReport não implementado. "
            "Implementar na tarefa 'Implementar gerador de relatório Excel'"
        )
