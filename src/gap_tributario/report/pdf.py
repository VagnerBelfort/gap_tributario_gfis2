"""Gerador de relatório PDF com ReportLab.

Gera relatório PDF contendo:
- Tabela de resultados do Gap Tributário
- Seção de metodologia VRR/OCDE
- Dados de entrada utilizados no cálculo

Arquivo gerado: {saida}/gap_icms_{periodo}.pdf
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, List

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.platypus.flowables import HRFlowable

if TYPE_CHECKING:
    from gap_tributario.models import AppConfig, DadosVRR, ResultadoGap

logger = logging.getLogger(__name__)

# Identidade visual Sefaz-MA
_COR_PRIMARIA = colors.HexColor("#1a3a6b")  # Azul institucional escuro
_COR_LINHA_PAR = colors.HexColor("#e8edf5")  # Azul muito claro para linhas alternadas
_COR_TEXTO_CLARO = colors.white


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
        nome_arquivo = f"gap_icms_{resultado.periodo.label}.pdf"

        try:
            caminho_saida.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise OSError(
                f"Não foi possível criar o diretório de saída '{caminho_saida}': {e}"
            ) from e

        arquivo = caminho_saida / nome_arquivo

        try:
            doc = SimpleDocTemplate(
                str(arquivo),
                pagesize=A4,
                leftMargin=2 * cm,
                rightMargin=2 * cm,
                topMargin=2 * cm,
                bottomMargin=2 * cm,
                compress=0,
            )
            story = self._construir_story(resultado, config)
            doc.build(story)
        except OSError as e:
            raise OSError(
                f"Não foi possível escrever o arquivo de saída '{arquivo}': {e}"
            ) from e

        tamanho = arquivo.stat().st_size
        logger.info("Relatório PDF gerado: %s (%d bytes)", arquivo, tamanho)

        return arquivo

    def _construir_story(
        self,
        resultado: "ResultadoGap",
        config: "AppConfig",
    ) -> List:
        """Constrói a lista de flowables para o documento PDF."""
        estilos = getSampleStyleSheet()
        largura_pagina = A4[0] - 4 * cm  # Largura útil descontando margens

        estilo_titulo = ParagraphStyle(
            "Titulo",
            parent=estilos["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=16,
            textColor=_COR_PRIMARIA,
            alignment=1,
            spaceAfter=4,
        )
        estilo_subtitulo = ParagraphStyle(
            "Subtitulo",
            parent=estilos["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            textColor=_COR_PRIMARIA,
            alignment=1,
            spaceAfter=2,
        )
        estilo_secao = ParagraphStyle(
            "Secao",
            parent=estilos["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=_COR_PRIMARIA,
            spaceBefore=12,
            spaceAfter=6,
        )
        estilo_normal = ParagraphStyle(
            "NormalCustom",
            parent=estilos["Normal"],
            fontName="Helvetica",
            fontSize=9,
            spaceAfter=3,
        )
        estilo_rodape = ParagraphStyle(
            "Rodape",
            parent=estilos["Normal"],
            fontName="Helvetica",
            fontSize=8,
            textColor=colors.grey,
            alignment=1,
        )

        data_geracao = datetime.now().strftime("%d/%m/%Y às %H:%M")
        story: List = []

        # === CABEÇALHO ===
        story.append(Paragraph("SECRETARIA DA FAZENDA DO MARANHÃO", estilo_titulo))
        story.append(Paragraph("RELATÓRIO DE GAP TRIBUTÁRIO DO ICMS", estilo_subtitulo))
        story.append(
            Paragraph(
                "Metodologia VRR — Value Added Tax Revenue Ratio (OCDE/ESAF-2012)",
                estilo_normal,
            )
        )
        story.append(Spacer(1, 0.3 * cm))
        story.append(HRFlowable(width="100%", thickness=2, color=_COR_PRIMARIA))
        story.append(Spacer(1, 0.3 * cm))

        # === IDENTIFICAÇÃO ===
        aliquota_vigente = config.get_aliquota(resultado.periodo)
        legislacao = next(
            (a.legislacao for a in config.aliquotas if a.aliquota == aliquota_vigente),
            "Legislação estadual vigente",
        )

        tabela_ident = Table(
            [
                ["Período de Referência:", resultado.periodo.label],
                ["Alíquota Modal Vigente:", _formatar_aliquota(aliquota_vigente)],
                ["Referência Legislativa:", legislacao],
                ["Data de Geração:", data_geracao],
            ],
            colWidths=[5 * cm, largura_pagina - 5 * cm],
        )
        tabela_ident.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                    ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("TEXTCOLOR", (0, 0), (0, -1), _COR_PRIMARIA),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                ]
            )
        )
        story.append(tabela_ident)
        story.append(Spacer(1, 0.3 * cm))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.lightgrey))
        story.append(Spacer(1, 0.2 * cm))

        # === 1. RESULTADOS DO CÁLCULO ===
        story.append(Paragraph("1. Resultados do Cálculo", estilo_secao))

        _estilo_tabela_resultados = [
            # Cabeçalho
            ("BACKGROUND", (0, 0), (-1, 0), _COR_PRIMARIA),
            ("TEXTCOLOR", (0, 0), (-1, 0), _COR_TEXTO_CLARO),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            # Linhas alternadas
            ("BACKGROUND", (0, 1), (-1, 1), colors.white),
            ("BACKGROUND", (0, 2), (-1, 2), _COR_LINHA_PAR),
            ("BACKGROUND", (0, 3), (-1, 3), colors.white),
            ("BACKGROUND", (0, 4), (-1, 4), _COR_LINHA_PAR),
            ("BACKGROUND", (0, 5), (-1, 5), colors.white),
            # Fontes de dados
            ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
            ("FONTNAME", (1, 1), (1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("ALIGN", (1, 1), (1, -1), "RIGHT"),
            # Bordas
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ("BOX", (0, 0), (-1, -1), 1, _COR_PRIMARIA),
            # Padding
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]

        tabela_resultados = Table(
            [
                ["Indicador", "Valor"],
                ["ICMS Arrecadado", _formatar_brl(resultado.icms_arrecadado)],
                ["ICMS Potencial (Base de Cálculo)", _formatar_brl(resultado.icms_potencial)],
                ["VRR — Value Added Tax Revenue Ratio", _formatar_vrr(resultado.vrr)],
                ["Gap Tributário Absoluto", _formatar_brl(resultado.gap_absoluto)],
                ["Gap Tributário Percentual", _formatar_percentual(resultado.gap_percentual)],
            ],
            colWidths=[largura_pagina * 0.65, largura_pagina * 0.35],
        )
        tabela_resultados.setStyle(TableStyle(_estilo_tabela_resultados))
        story.append(tabela_resultados)
        story.append(Spacer(1, 0.4 * cm))

        # === 2. DADOS DE ENTRADA ===
        story.append(Paragraph("2. Dados de Entrada Utilizados", estilo_secao))

        tabela_dados = Table(
            [
                ["Parâmetro", "Valor"],
                ["VAB — Valor Adicionado Bruto (IBGE SIDRA)", _formatar_brl(resultado.vab)],
                [
                    "Exportações FOB (MDIC ComEx, em BRL)",
                    _formatar_brl(resultado.exportacoes_brl),
                ],
                [
                    "Importações FOB (MDIC ComEx, em BRL)",
                    _formatar_brl(resultado.importacoes_brl),
                ],
                ["Cotação PTAX Média USD/BRL (BCB)", _formatar_ptax(resultado.ptax_media)],
                ["Alíquota Modal ICMS-MA", _formatar_aliquota(resultado.aliquota_padrao)],
            ],
            colWidths=[largura_pagina * 0.65, largura_pagina * 0.35],
        )
        tabela_dados.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), _COR_PRIMARIA),
                    ("TEXTCOLOR", (0, 0), (-1, 0), _COR_TEXTO_CLARO),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("BACKGROUND", (0, 1), (-1, 1), colors.white),
                    ("BACKGROUND", (0, 2), (-1, 2), _COR_LINHA_PAR),
                    ("BACKGROUND", (0, 3), (-1, 3), colors.white),
                    ("BACKGROUND", (0, 4), (-1, 4), _COR_LINHA_PAR),
                    ("BACKGROUND", (0, 5), (-1, 5), colors.white),
                    ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
                    ("FONTNAME", (1, 1), (1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 9),
                    ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
                    ("BOX", (0, 0), (-1, -1), 1, _COR_PRIMARIA),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        story.append(tabela_dados)
        story.append(Spacer(1, 0.4 * cm))

        # === 3. METODOLOGIA E REFERÊNCIAS ===
        story.append(Paragraph("3. Metodologia e Referências", estilo_secao))
        story.append(Paragraph("<b>Fórmula VRR (Value Added Tax Revenue Ratio):</b>", estilo_normal))
        story.append(Spacer(1, 0.2 * cm))

        tabela_formula = Table(
            [["VRR = ICMS Arrecadado / [(VAB - Exportacoes + Importacoes) x Aliquota]"]],
            colWidths=[largura_pagina],
        )
        tabela_formula.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), _COR_LINHA_PAR),
                    ("FONTNAME", (0, 0), (-1, -1), "Courier-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("BOX", (0, 0), (-1, -1), 1, _COR_PRIMARIA),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        story.append(tabela_formula)
        story.append(Spacer(1, 0.3 * cm))

        itens_metodologia = [
            (
                "<b>Fonte Metodologica:</b> ESAF (2012) — "
                "<i>Estimativa da carga tributaria potencial e da evasao fiscal no ICMS</i>."
            ),
            (
                "<b>Referencia OCDE:</b> OECD Revenue Statistics — "
                "VRR medio OCDE = 0,58 (valor de referencia internacional)."
            ),
            (
                "<b>Benchmark Europeu:</b> Gap medio de conformidade EU aproximadamente 9,5% "
                "(European Commission, VAT Gap Report)."
            ),
            (
                "<b>Interpretacao do VRR:</b> Valores proximos a 1,0 indicam maior eficiencia "
                "arrecadatoria. VRR abaixo de 0,58 (media OCDE) sinaliza gap tributario relevante."
            ),
            (
                "<b>Base de Dados:</b> ICMS arrecadado — GFIS2/Sefaz-MA; "
                "VAB — IBGE SIDRA (Tabela 5938); Comercio exterior — MDIC ComEx; "
                "Cambio — BCB PTAX."
            ),
        ]

        for item in itens_metodologia:
            story.append(Paragraph(f"&#x2022; {item}", estilo_normal))

        story.append(Spacer(1, 0.5 * cm))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.lightgrey))
        story.append(Spacer(1, 0.2 * cm))

        story.append(
            Paragraph(
                f"Relatorio gerado em {data_geracao} — "
                "Sistema Gap Tributario ICMS-MA v1.0 — "
                "Secretaria da Fazenda do Maranhao",
                estilo_rodape,
            )
        )

        return story
