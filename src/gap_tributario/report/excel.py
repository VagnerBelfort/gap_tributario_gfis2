"""Gerador de relatório Excel com XlsxWriter.

Gera planilha Excel contendo:
- Tabela de resultados do Gap Tributário
- Campos do contrato ResultadoGap em células nomeadas
- Seção de metodologia VRR/OCDE

Arquivo gerado: {saida}/gap_icms_{periodo}.xlsx
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import xlsxwriter

if TYPE_CHECKING:
    from gap_tributario.models import AppConfig, DadosVRR, ResultadoGap

logger = logging.getLogger(__name__)

# Identidade visual Sefaz-MA
_COR_PRIMARIA = "#1a3a6b"  # Azul institucional escuro
_COR_LINHA_PAR = "#e8edf5"  # Azul muito claro para linhas alternadas
_COR_TEXTO_CLARO = "#ffffff"


def _formatar_brl(valor: Decimal) -> str:
    """Formata Decimal no padrão brasileiro: R$ 10.148,22 milhões."""
    valor_round = valor.quantize(Decimal("0.01"))
    sinal = "-" if valor_round < 0 else ""
    magnitude = abs(valor_round)
    str_val = str(magnitude)
    if "." in str_val:
        int_part, dec_part = str_val.split(".")
        dec_part = dec_part[:2].ljust(2, "0")
    else:
        int_part = str_val
        dec_part = "00"
    int_fmt = f"{int(int_part):,}".replace(",", ".")
    return f"{sinal}R$ {int_fmt},{dec_part} milhões"


def _formatar_vrr(valor: Decimal) -> str:
    """Formata VRR com 4 casas decimais no padrão brasileiro."""
    valor_round = valor.quantize(Decimal("0.0001"))
    str_val = str(valor_round)
    if "." in str_val:
        int_part, dec_part = str_val.split(".")
        dec_part = dec_part[:4].ljust(4, "0")
    else:
        int_part = str_val
        dec_part = "0000"
    return f"{int_part},{dec_part}"


def _formatar_percentual(valor: Decimal) -> str:
    """Formata percentual no padrão brasileiro: 48,00%."""
    valor_round = valor.quantize(Decimal("0.01"))
    str_val = str(valor_round)
    if "." in str_val:
        int_part, dec_part = str_val.split(".")
        dec_part = dec_part[:2].ljust(2, "0")
    else:
        int_part = str_val
        dec_part = "00"
    return f"{int_part},{dec_part}%"


def _formatar_aliquota(valor: Decimal) -> str:
    """Formata alíquota como percentual: 18,00%."""
    return _formatar_percentual(valor * Decimal("100"))


def _formatar_ptax(valor: Decimal) -> str:
    """Formata cotação PTAX no padrão brasileiro: R$ 5,2000."""
    valor_round = valor.quantize(Decimal("0.0001"))
    str_val = str(valor_round)
    if "." in str_val:
        int_part, dec_part = str_val.split(".")
        dec_part = dec_part[:4].ljust(4, "0")
    else:
        int_part = str_val
        dec_part = "0000"
    return f"R$ {int_part},{dec_part}"


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
        nome_arquivo = f"gap_icms_{resultado.periodo.label}.xlsx"

        try:
            caminho_saida.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise OSError(
                f"Não foi possível criar o diretório de saída '{caminho_saida}': {e}"
            ) from e

        arquivo = caminho_saida / nome_arquivo

        try:
            self._gerar_workbook(resultado, config, arquivo)
        except OSError as e:
            raise OSError(f"Não foi possível escrever o arquivo de saída '{arquivo}': {e}") from e

        tamanho = arquivo.stat().st_size
        logger.info("Relatório Excel gerado: %s (%d bytes)", arquivo, tamanho)

        return arquivo

    def _gerar_workbook(
        self,
        resultado: "ResultadoGap",
        config: "AppConfig",
        arquivo: Path,
    ) -> None:
        """Cria o workbook Excel com todas as seções."""
        workbook = xlsxwriter.Workbook(str(arquivo))

        fmt_titulo = workbook.add_format(
            {
                "bold": True,
                "font_size": 14,
                "font_color": _COR_TEXTO_CLARO,
                "bg_color": _COR_PRIMARIA,
                "align": "center",
                "valign": "vcenter",
            }
        )
        fmt_secao = workbook.add_format(
            {
                "bold": True,
                "font_size": 11,
                "font_color": _COR_TEXTO_CLARO,
                "bg_color": _COR_PRIMARIA,
            }
        )
        fmt_label = workbook.add_format(
            {
                "bold": True,
                "font_color": _COR_PRIMARIA,
            }
        )
        fmt_valor = workbook.add_format({"align": "right"})
        fmt_linha_par = workbook.add_format(
            {
                "bg_color": _COR_LINHA_PAR,
                "align": "right",
            }
        )
        fmt_label_par = workbook.add_format(
            {
                "bold": True,
                "font_color": _COR_PRIMARIA,
                "bg_color": _COR_LINHA_PAR,
            }
        )
        fmt_formula = workbook.add_format(
            {
                "bg_color": _COR_LINHA_PAR,
                "bold": True,
                "align": "center",
            }
        )
        fmt_cabecalho_label = workbook.add_format(
            {
                "bold": True,
                "font_color": _COR_PRIMARIA,
            }
        )
        fmt_cabecalho_valor = workbook.add_format({})

        ws = workbook.add_worksheet("Relatório")
        ws.set_column(0, 0, 50)
        ws.set_column(1, 1, 35)

        aliquota_vigente = config.get_aliquota(resultado.periodo)
        legislacao = next(
            (a.legislacao for a in config.aliquotas if a.aliquota == aliquota_vigente),
            "Legislação estadual vigente",
        )

        data_geracao = datetime.now().strftime("%d/%m/%Y às %H:%M")

        linha = 0

        # === CABEÇALHO ===
        ws.merge_range(linha, 0, linha, 1, "SECRETARIA DA FAZENDA DO MARANHÃO", fmt_titulo)
        linha += 1
        ws.merge_range(linha, 0, linha, 1, "RELATÓRIO DE GAP TRIBUTÁRIO DO ICMS", fmt_titulo)
        linha += 1
        ws.merge_range(
            linha,
            0,
            linha,
            1,
            "Metodologia VRR — Value Added Tax Revenue Ratio (OCDE/ESAF-2012)",
            fmt_titulo,
        )
        linha += 2

        # === IDENTIFICAÇÃO ===
        ws.write(linha, 0, "Período de Referência:", fmt_cabecalho_label)
        ws.write(linha, 1, resultado.periodo.label, fmt_cabecalho_valor)
        linha += 1
        ws.write(linha, 0, "Alíquota Modal Vigente:", fmt_cabecalho_label)
        ws.write(linha, 1, _formatar_aliquota(aliquota_vigente), fmt_cabecalho_valor)
        linha += 1
        ws.write(linha, 0, "Referência Legislativa:", fmt_cabecalho_label)
        ws.write(linha, 1, legislacao, fmt_cabecalho_valor)
        linha += 1
        ws.write(linha, 0, "Data de Geração:", fmt_cabecalho_label)
        ws.write(linha, 1, data_geracao, fmt_cabecalho_valor)
        linha += 2

        # === 1. RESULTADOS DO CÁLCULO ===
        ws.merge_range(linha, 0, linha, 1, "1. Resultados do Cálculo", fmt_secao)
        linha += 1

        dados_resultados = [
            ("ICMS Arrecadado", _formatar_brl(resultado.icms_arrecadado)),
            ("ICMS Potencial (Base de Cálculo)", _formatar_brl(resultado.icms_potencial)),
            ("VRR — Value Added Tax Revenue Ratio", _formatar_vrr(resultado.vrr)),
            ("Gap Tributário Absoluto", _formatar_brl(resultado.gap_absoluto)),
            ("Gap Tributário Percentual", _formatar_percentual(resultado.gap_percentual)),
        ]

        for i, (indicador, valor) in enumerate(dados_resultados):
            if i % 2 == 0:
                ws.write(linha, 0, indicador, fmt_label)
                ws.write(linha, 1, valor, fmt_valor)
            else:
                ws.write(linha, 0, indicador, fmt_label_par)
                ws.write(linha, 1, valor, fmt_linha_par)
            linha += 1

        linha += 1

        # === 2. DADOS DE ENTRADA ===
        ws.merge_range(linha, 0, linha, 1, "2. Dados de Entrada Utilizados", fmt_secao)
        linha += 1

        dados_entrada = [
            ("VAB — Valor Adicionado Bruto (IBGE SIDRA)", _formatar_brl(resultado.vab)),
            ("Exportações FOB (MDIC ComEx, em BRL)", _formatar_brl(resultado.exportacoes_brl)),
            ("Importações FOB (MDIC ComEx, em BRL)", _formatar_brl(resultado.importacoes_brl)),
            ("Cotação PTAX Média USD/BRL (BCB)", _formatar_ptax(resultado.ptax_media)),
            ("Alíquota Modal ICMS-MA", _formatar_aliquota(resultado.aliquota_padrao)),
        ]

        for i, (param, valor) in enumerate(dados_entrada):
            if i % 2 == 0:
                ws.write(linha, 0, param, fmt_label)
                ws.write(linha, 1, valor, fmt_valor)
            else:
                ws.write(linha, 0, param, fmt_label_par)
                ws.write(linha, 1, valor, fmt_linha_par)
            linha += 1

        linha += 1

        # === 3. METODOLOGIA E REFERÊNCIAS ===
        ws.merge_range(linha, 0, linha, 1, "3. Metodologia e Referências", fmt_secao)
        linha += 1

        ws.merge_range(
            linha,
            0,
            linha,
            1,
            "VRR = ICMS Arrecadado / [(VAB - Exportacoes + Importacoes) x Aliquota]",
            fmt_formula,
        )
        linha += 2

        itens_metodologia = [
            (
                "Fonte Metodologica: ESAF (2012) — Estimativa da carga tributaria potencial "
                "e da evasao fiscal no ICMS."
            ),
            (
                "Referencia OCDE: OECD Revenue Statistics — VRR medio OCDE = 0,58 "
                "(valor de referencia internacional)."
            ),
            (
                "Benchmark Europeu: Gap medio de conformidade EU aproximadamente 9,5% "
                "(European Commission, VAT Gap Report)."
            ),
            (
                "Interpretacao do VRR: Valores proximos a 1,0 indicam maior eficiencia "
                "arrecadatoria. VRR abaixo de 0,58 (media OCDE) sinaliza gap tributario relevante."
            ),
            (
                "Base de Dados: ICMS arrecadado — GFIS2/Sefaz-MA; VAB — IBGE SIDRA (Tabela 5938); "
                "Comercio exterior — MDIC ComEx; Cambio — BCB PTAX."
            ),
        ]

        for item in itens_metodologia:
            ws.merge_range(linha, 0, linha, 1, item)
            linha += 1

        linha += 1
        ws.merge_range(
            linha,
            0,
            linha,
            1,
            f"Relatorio gerado em {data_geracao} — "
            "Sistema Gap Tributario ICMS-MA v1.0 — Secretaria da Fazenda do Maranhao",
        )

        workbook.close()
