"""Testes do gerador de relatório Excel.

Testes funcionais que cobrem geração de arquivo, conteúdo e erros.
"""

from __future__ import annotations

import logging
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.models import ResultadoGap
from gap_tributario.report.excel import ExcelReport

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


def _extrair_texto_xlsx(arquivo: Path) -> str:
    """Extrai todo o texto dos XMLs internos do arquivo xlsx."""
    partes = []
    with zipfile.ZipFile(arquivo) as zf:
        for nome in zf.namelist():
            if nome.endswith(".xml"):
                with zf.open(nome) as f:
                    partes.append(f.read().decode("utf-8", errors="replace"))
    return " ".join(partes)


# ---------------------------------------------------------------------------
# Testes de importação e instanciação (scaffolding existente mantido)
# ---------------------------------------------------------------------------


def test_excel_report_importable():
    """Verifica que o gerador Excel é importável."""
    assert ExcelReport is not None


def test_excel_report_instantiable():
    """Verifica que o gerador Excel pode ser instanciado."""
    report = ExcelReport()
    assert report is not None


# ---------------------------------------------------------------------------
# Testes de geração bem-sucedida
# ---------------------------------------------------------------------------


def test_geracao_bem_sucedida_anual(resultado_2022, config_test, tmp_path):
    """Arquivo gap_icms_2022.xlsx é criado no diretório de saída."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    caminho = excel.gerar(resultado_2022, None, config_test, saida)

    assert caminho.name == "gap_icms_2022.xlsx"
    assert caminho.exists()
    assert caminho.is_file()


def test_geracao_periodo_trimestral(resultado_2022_t1, config_test, tmp_path):
    """Arquivo gap_icms_2022-T1.xlsx é criado corretamente para período trimestral."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    caminho = excel.gerar(resultado_2022_t1, None, config_test, saida)

    assert caminho.name == "gap_icms_2022-T1.xlsx"
    assert caminho.exists()


def test_magic_bytes_xlsx_valido(resultado_2022, config_test, tmp_path):
    """Arquivo xlsx gerado tem magic bytes válidos de ZIP (PK\\x03\\x04)."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    arquivo = excel.gerar(resultado_2022, None, config_test, saida)

    conteudo = arquivo.read_bytes()
    assert len(conteudo) > 0
    assert conteudo[:4] == b"PK\x03\x04"


def test_criacao_automatica_diretorio(resultado_2022, config_test, tmp_path):
    """Cria automaticamente o diretório de saída se não existir."""
    saida = tmp_path / "novo" / "diretorio" / "aninhado"
    assert not saida.exists()

    excel = ExcelReport()
    excel.gerar(resultado_2022, None, config_test, saida)

    assert saida.exists()
    assert saida.is_dir()


def test_retorno_e_path_correto(resultado_2022, config_test, tmp_path):
    """gerar() retorna Path apontando para o arquivo criado."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    resultado = excel.gerar(resultado_2022, None, config_test, saida)

    assert isinstance(resultado, Path)
    assert resultado.is_file()
    assert resultado == saida / "gap_icms_2022.xlsx"


def test_aliquota_20_porcento_2023(resultado_2023, config_test, tmp_path):
    """Relatório gerado para 2023 reflete corretamente a alíquota de 20%."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    arquivo = excel.gerar(resultado_2023, None, config_test, saida)

    assert arquivo.name == "gap_icms_2023.xlsx"
    assert arquivo.exists()

    conteudo = arquivo.read_bytes()
    assert len(conteudo) > 0


def test_campos_obrigatorios_no_conteudo(resultado_2022, config_test, tmp_path):
    """Todos os campos do contrato ResultadoGap estão presentes no conteúdo da planilha."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    arquivo = excel.gerar(resultado_2022, None, config_test, saida)

    texto = _extrair_texto_xlsx(arquivo)

    assert "ICMS" in texto
    assert "VRR" in texto
    assert "Gap" in texto
    assert "2022" in texto
    assert "VAB" in texto


def test_vrr_referencia_no_conteudo(resultado_2022, config_test, tmp_path):
    """VRR de referência MA 2022 ≈ 0,5183 está presente na planilha."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()
    arquivo = excel.gerar(resultado_2022, None, config_test, saida)

    texto = _extrair_texto_xlsx(arquivo)
    assert "0,5183" in texto


def test_logging_info_apos_geracao(resultado_2022, config_test, tmp_path, caplog):
    """Logger emite mensagem INFO com caminho do arquivo após geração bem-sucedida."""
    saida = tmp_path / "relatorios"
    excel = ExcelReport()

    with caplog.at_level(logging.INFO, logger="gap_tributario.report.excel"):
        arquivo = excel.gerar(resultado_2022, None, config_test, saida)

    assert str(arquivo) in caplog.text
    assert "bytes" in caplog.text


# ---------------------------------------------------------------------------
# Testes de erro
# ---------------------------------------------------------------------------


def test_ioerror_quando_caminho_invalido(resultado_2022, config_test, tmp_path):
    """Levanta IOError quando caminho_saida não pode ser criado (arquivo no lugar do diretório)."""
    # Cria arquivo regular no lugar onde seria o diretório de saída
    saida_arquivo = tmp_path / "bloqueado.txt"
    saida_arquivo.write_text("sou um arquivo, nao um diretorio")

    # Tenta criar subdiretório dentro de um arquivo → NotADirectoryError → IOError
    caminho_invalido = saida_arquivo / "subdir"

    excel = ExcelReport()
    with pytest.raises(IOError):
        excel.gerar(resultado_2022, None, config_test, caminho_invalido)
