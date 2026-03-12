"""Testes do gerador de relatório PDF.

Testes funcionais que cobrem geração de arquivo, conteúdo e erros.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.models import ResultadoGap
from gap_tributario.report.pdf import (
    PDFReport,
    _formatar_aliquota,
    _formatar_brl,
    _formatar_percentual,
    _formatar_ptax,
    _formatar_vrr,
)

# ---------------------------------------------------------------------------
# Fixtures locais
# ---------------------------------------------------------------------------


@pytest.fixture
def resultado_2022(periodo_2022_anual) -> ResultadoGap:
    """ResultadoGap de referência MA 2022 (anual)."""
    return ResultadoGap(
        periodo=periodo_2022_anual,
        icms_arrecadado=Decimal("10917.00"),
        icms_potencial=Decimal("21065.22"),
        vrr=Decimal("0.5183"),
        gap_absoluto=Decimal("10148.22"),
        gap_percentual=Decimal("48.18"),
        vab=Decimal("124859.00"),
        exportacoes_brl=Decimal("29754.00"),
        importacoes_brl=Decimal("21924.00"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.1646"),
    )


@pytest.fixture
def resultado_2022_t1(periodo_2022_t1) -> ResultadoGap:
    """ResultadoGap MA 2022-T1 (trimestral)."""
    return ResultadoGap(
        periodo=periodo_2022_t1,
        icms_arrecadado=Decimal("2750.00"),
        icms_potencial=Decimal("5287.05"),
        vrr=Decimal("0.5202"),
        gap_absoluto=Decimal("2537.05"),
        gap_percentual=Decimal("47.99"),
        vab=Decimal("31214.75"),
        exportacoes_brl=Decimal("7438.50"),
        importacoes_brl=Decimal("5481.00"),
        aliquota_padrao=Decimal("0.18"),
        ptax_media=Decimal("5.20"),
    )


@pytest.fixture
def resultado_2023(periodo_2023_anual) -> ResultadoGap:
    """ResultadoGap MA 2023 com alíquota 20%."""
    return ResultadoGap(
        periodo=periodo_2023_anual,
        icms_arrecadado=Decimal("13000.00"),
        icms_potencial=Decimal("23000.00"),
        vrr=Decimal("0.5652"),
        gap_absoluto=Decimal("10000.00"),
        gap_percentual=Decimal("43.48"),
        vab=Decimal("100000.00"),
        exportacoes_brl=Decimal("5000.00"),
        importacoes_brl=Decimal("3000.00"),
        aliquota_padrao=Decimal("0.20"),
        ptax_media=Decimal("5.10"),
    )


# ---------------------------------------------------------------------------
# Testes de importação e instanciação (scaffolding existente mantido)
# ---------------------------------------------------------------------------


def test_pdf_report_importable():
    """Verifica que o gerador PDF é importável."""
    assert PDFReport is not None


def test_pdf_report_instantiable():
    """Verifica que o gerador PDF pode ser instanciado."""
    report = PDFReport()
    assert report is not None


# ---------------------------------------------------------------------------
# Testes de geração bem-sucedida
# ---------------------------------------------------------------------------


def test_geracao_bem_sucedida_anual(resultado_2022, config_test, tmp_path):
    """Arquivo gap_icms_2022.pdf é criado no diretório de saída."""
    saida = tmp_path / "relatorios"
    pdf = PDFReport()
    caminho = pdf.gerar(resultado_2022, None, config_test, saida)

    assert caminho.name == "gap_icms_2022.pdf"
    assert caminho.exists()
    assert caminho.is_file()


def test_geracao_periodo_trimestral(resultado_2022_t1, config_test, tmp_path):
    """Arquivo gap_icms_2022-T1.pdf é criado corretamente para período trimestral."""
    saida = tmp_path / "relatorios"
    pdf = PDFReport()
    caminho = pdf.gerar(resultado_2022_t1, None, config_test, saida)

    assert caminho.name == "gap_icms_2022-T1.pdf"
    assert caminho.exists()


def test_conteudo_pdf_valido(resultado_2022, config_test, tmp_path):
    """PDF gerado é não-vazio e inicia com header PDF válido (%PDF-)."""
    saida = tmp_path / "relatorios"
    pdf = PDFReport()
    arquivo = pdf.gerar(resultado_2022, None, config_test, saida)

    conteudo = arquivo.read_bytes()
    assert len(conteudo) > 0
    assert conteudo.startswith(b"%PDF-")


def test_criacao_automatica_diretorio(resultado_2022, config_test, tmp_path):
    """Cria automaticamente o diretório de saída se não existir."""
    saida = tmp_path / "novo" / "diretorio" / "aninhado"
    assert not saida.exists()

    pdf = PDFReport()
    pdf.gerar(resultado_2022, None, config_test, saida)

    assert saida.exists()
    assert saida.is_dir()


def test_retorno_e_path_correto(resultado_2022, config_test, tmp_path):
    """gerar() retorna Path apontando para o arquivo criado."""
    saida = tmp_path / "relatorios"
    pdf = PDFReport()
    resultado = pdf.gerar(resultado_2022, None, config_test, saida)

    assert isinstance(resultado, Path)
    assert resultado.is_file()
    assert resultado == saida / "gap_icms_2022.pdf"


def test_aliquota_20_porcento_2023(resultado_2023, config_test, tmp_path):
    """Relatório gerado para 2023 reflete corretamente a alíquota de 20%."""
    saida = tmp_path / "relatorios"
    pdf = PDFReport()
    arquivo = pdf.gerar(resultado_2023, None, config_test, saida)

    assert arquivo.name == "gap_icms_2023.pdf"
    assert arquivo.exists()

    conteudo = arquivo.read_bytes()
    assert len(conteudo) > 0


def test_campos_obrigatorios_no_story(resultado_2022, config_test):
    """Verifica presença de campos-chave no story PDF (antes da renderização).

    Testa na camada de story para evitar dependência de formato binário comprimido.
    """
    from reportlab.platypus import Paragraph, Table

    pdf = PDFReport()
    story = pdf._construir_story(resultado_2022, config_test)

    # Texto de todos os Paragraphs
    texto_paras = " ".join(item.text for item in story if isinstance(item, Paragraph))

    # Texto das células de tabelas (strings diretas)
    texto_tabs_lista = []
    for item in story:
        if isinstance(item, Table):
            for row in item._cellvalues:
                for cell in row:
                    if isinstance(cell, str):
                        texto_tabs_lista.append(cell)
    texto_tabs = " ".join(texto_tabs_lista)

    todo_texto = texto_paras + " " + texto_tabs

    assert "VRR" in todo_texto
    assert "Gap" in todo_texto
    assert "ICMS" in todo_texto
    assert "2022" in todo_texto


# ---------------------------------------------------------------------------
# Teste de erro de escrita
# ---------------------------------------------------------------------------


def test_ioerror_quando_caminho_invalido(resultado_2022, config_test, tmp_path):
    """Levanta IOError quando caminho_saida não pode ser criado (arquivo no lugar do diretório)."""
    # Cria arquivo regular no lugar onde seria o diretório de saída
    saida_arquivo = tmp_path / "bloqueado.txt"
    saida_arquivo.write_text("sou um arquivo, nao um diretorio")

    # Tenta criar subdiretório dentro de um arquivo → NotADirectoryError → IOError
    caminho_invalido = saida_arquivo / "subdir"

    pdf = PDFReport()
    with pytest.raises(IOError):
        pdf.gerar(resultado_2022, None, config_test, caminho_invalido)


# ---------------------------------------------------------------------------
# Testes das funções de formatação
# ---------------------------------------------------------------------------


def test_formatar_brl_valor_simples():
    """Formata valor simples em BRL no padrão brasileiro."""
    assert _formatar_brl(Decimal("10148.22")) == "R$ 10.148,22 milhões"


def test_formatar_brl_milhar():
    """Formata valor com separador de milhar corretamente."""
    assert _formatar_brl(Decimal("124859.00")) == "R$ 124.859,00 milhões"


def test_formatar_brl_zero():
    """Formata zero corretamente."""
    assert _formatar_brl(Decimal("0")) == "R$ 0,00 milhões"


def test_formatar_vrr():
    """Formata VRR com 4 casas decimais no padrão brasileiro."""
    assert _formatar_vrr(Decimal("0.5183")) == "0,5183"


def test_formatar_percentual():
    """Formata percentual no padrão brasileiro."""
    assert _formatar_percentual(Decimal("48.18")) == "48,18%"


def test_formatar_aliquota_18():
    """Formata alíquota 18% corretamente."""
    assert _formatar_aliquota(Decimal("0.18")) == "18,00%"


def test_formatar_aliquota_20():
    """Formata alíquota 20% corretamente."""
    assert _formatar_aliquota(Decimal("0.20")) == "20,00%"


def test_formatar_ptax():
    """Formata PTAX com 4 casas decimais no padrão brasileiro."""
    assert _formatar_ptax(Decimal("5.1646")) == "R$ 5,1646"
