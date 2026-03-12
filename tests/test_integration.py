"""Testes de integração do pipeline completo.

Verifica o pipeline end-to-end com fixtures locais (Parquet, CSV)
e mocks de APIs externas (IBGE SIDRA, BCB PTAX).

Referência MA 2022:
    ICMS=10917, VAB=124859, Exp=29754, Imp=21924, Alíq=0.18, PTAX=5.22
    ICMS Potencial = (124859 - 29754 + 21924) × 0.18 = 21065.22
    VRR = 10917 / 21065.22 ≈ 0.5183 ≈ 0.52
    Gap = 21065.22 - 10917 = 10148.22
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from gap_tributario.cli import run

# Caminhos das fixtures
_FIXTURES = Path(__file__).parent / "fixtures"
_FIXTURE_PARQUET = _FIXTURES / "parquet" / "arrecadacao_fixture.parquet"
_FIXTURE_EXP_CSV = _FIXTURES / "csv" / "EXP_2022.csv"
_FIXTURE_IMP_CSV = _FIXTURES / "csv" / "IMP_2022.csv"

# Valores de referência MA 2022
_PTAX_REF = Decimal("5.22")
_VAB_REF = Decimal("124859")
_ICMS_REF = Decimal("10917")
_EXP_REF = Decimal("29754")
_IMP_REF = Decimal("21924")
_ALIQUOTA_REF = Decimal("0.18")


# ---------------------------------------------------------------------------
# Fixtures de configuração
# ---------------------------------------------------------------------------


@pytest.fixture
def config_integracao(tmp_path: Path) -> Path:
    """Cria config de integração com paths absolutos para as fixtures."""
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    shutil.copy(str(_FIXTURE_PARQUET), str(parquet_dir / "arrecadacao_fixture.parquet"))

    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    shutil.copy(str(_FIXTURE_EXP_CSV), str(csv_dir / "EXP_2022.csv"))
    shutil.copy(str(_FIXTURE_IMP_CSV), str(csv_dir / "IMP_2022.csv"))

    config_content = f"""aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Lei Estadual vigente até 2022"
  - ano_inicio: 2023
    ano_fim: null
    aliquota: 0.20
    legislacao: "Lei 11.867/2022 — alíquota modal 20%"

fontes:
  parquet_base_path: "{parquet_dir}"
  mdic_base_path: "{csv_dir}"

oracle:
  dsn: ""
  user: ""
  password: ""
"""
    config_file = tmp_path / "aliquotas.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    return config_file


@pytest.fixture
def saida_dir(tmp_path: Path) -> Path:
    """Diretório de saída para relatórios."""
    d = tmp_path / "output"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _pipeline_referencia(config_path: Path, output_path: Path, formato: str = "pdf excel") -> int:
    """Executa pipeline com valores de referência MA 2022 (todos extractors mockados)."""
    formatos = formato.split()
    argv = [
        "gap-tributario",
        "--periodo",
        "2022",
        "--config",
        str(config_path),
        "--saida",
        str(output_path),
        "--formato",
    ] + formatos

    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX_REF,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        return_value=_VAB_REF,
    ), patch(
        "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
        return_value=_ICMS_REF,
    ), patch(
        "gap_tributario.extractors.comex.ComexExtractor.extract",
        return_value=(_EXP_REF, _IMP_REF),
    ):
        with patch("sys.argv", argv):
            return run()


# ---------------------------------------------------------------------------
# Teste 1: Pipeline com dados de referência → VRR ≈ 0,52
# ---------------------------------------------------------------------------


def test_pipeline_completo_vrr_aprox_052(config_integracao, saida_dir):
    """Pipeline completo com dados MA 2022 produz VRR ≈ 0,52 (±0,01).

    Todos os extractors são mockados com os valores de referência ENCAT:
    - PTAX média 2022: 5.22 R$/USD
    - VAB MA 2022: 124.859 milhões R$
    - ICMS arrecadado: 10.917 milhões R$
    - Exportações: 29.754 milhões R$
    - Importações: 21.924 milhões R$

    VRR esperado:
        base = 124.859 - 29.754 + 21.924 = 117.029
        potencial = 117.029 × 0.18 = 21.065,22
        VRR = 10.917 / 21.065,22 ≈ 0,5183
    """
    result = _pipeline_referencia(config_integracao, saida_dir, "pdf")
    assert result == 0

    # Verifica VRR diretamente via motor de cálculo
    from gap_tributario.engine.vrr import MotorVRR
    from gap_tributario.models import DadosVRR, PeriodoCalculo

    dados = DadosVRR(
        periodo=PeriodoCalculo(ano=2022),
        icms_arrecadado=_ICMS_REF,
        vab=_VAB_REF,
        exportacoes_brl=_EXP_REF,
        importacoes_brl=_IMP_REF,
        aliquota_padrao=_ALIQUOTA_REF,
        ptax_media=_PTAX_REF,
    )
    resultado = MotorVRR().calcular(dados)

    vrr_float = float(resultado.vrr)
    assert abs(vrr_float - 0.52) <= 0.01, (
        f"VRR {vrr_float:.4f} deve ser ≈ 0,52 (±0,01). "
        f"Dados: ICMS={_ICMS_REF}, VAB={_VAB_REF}, "
        f"Exp={_EXP_REF}, Imp={_IMP_REF}, Alíq={_ALIQUOTA_REF}"
    )


# ---------------------------------------------------------------------------
# Teste 2: Relatório PDF contém as 3 seções obrigatórias
# ---------------------------------------------------------------------------


def test_relatorio_pdf_contem_secoes_obrigatorias(config_integracao, saida_dir):
    """Relatório PDF gerado contém as 3 seções obrigatórias da metodologia VRR.

    As seções são verificadas capturando o story do PDFReport._construir_story,
    que contém os Paragraphs com os títulos das seções.
    """
    from gap_tributario.report.pdf import PDFReport

    # Captura o story gerado para verificar as seções
    original_construir_story = PDFReport._construir_story
    captured_story: list = []

    def mock_construir_story(self, resultado, config):  # type: ignore[no-untyped-def]
        story = original_construir_story(self, resultado, config)
        captured_story.extend(story)
        return story

    with patch.object(PDFReport, "_construir_story", mock_construir_story):
        result = _pipeline_referencia(config_integracao, saida_dir, "pdf")

    assert result == 0

    pdf_file = saida_dir / "gap_icms_2022.pdf"
    assert pdf_file.exists(), "Arquivo PDF não foi gerado"
    assert pdf_file.stat().st_size > 0, "Arquivo PDF está vazio"
    # Verifica que é um arquivo PDF válido
    assert pdf_file.read_bytes().startswith(b"%PDF-"), "Arquivo não é um PDF válido"

    # Verifica as 3 seções obrigatórias no story
    from reportlab.platypus import Paragraph

    textos_paragrafos = [
        getattr(item, "text", "") for item in captured_story if isinstance(item, Paragraph)
    ]

    assert any("Resultados do" in t for t in textos_paragrafos), (
        f"Seção '1. Resultados do Cálculo' não encontrada. Parágrafos: {textos_paragrafos[:10]}"
    )
    assert any("Dados de Entrada" in t for t in textos_paragrafos), (
        "Seção '2. Dados de Entrada Utilizados' não encontrada"
    )
    assert any("Metodologia" in t for t in textos_paragrafos), (
        "Seção '3. Metodologia e Referências' não encontrada"
    )


# ---------------------------------------------------------------------------
# Teste 3: Relatório Excel contém campos do contrato ResultadoGap
# ---------------------------------------------------------------------------


def test_relatorio_excel_contem_campos_resultado(config_integracao, saida_dir):
    """Relatório Excel gerado contém os campos do contrato ResultadoGap."""
    result = _pipeline_referencia(config_integracao, saida_dir, "excel")
    assert result == 0

    xlsx_file = saida_dir / "gap_icms_2022.xlsx"
    assert xlsx_file.exists(), "Arquivo Excel não foi gerado"
    assert xlsx_file.stat().st_size > 0, "Arquivo Excel está vazio"

    # XLSX é ZIP com XMLs internos — lê conteúdo de todos os arquivos internos
    with zipfile.ZipFile(xlsx_file) as zf:
        xml_content = b"".join(zf.read(name) for name in zf.namelist())

    assert b"VRR" in xml_content, "Campo VRR não encontrado no arquivo Excel"
    assert b"ICMS" in xml_content, "Campo ICMS não encontrado no arquivo Excel"


# ---------------------------------------------------------------------------
# Teste 4: Pipeline com fixtures locais (Parquet + CSV) e mocks de API
# ---------------------------------------------------------------------------


def test_pipeline_com_fixtures_locais_e_mocks_api(config_integracao, saida_dir):
    """Pipeline com fixtures locais de Parquet/CSV e mocks de IBGE/BCB → exit(0).

    Usa:
    - ArrecadacaoExtractor com fixture Parquet (ICMS ≈ 9.8 M)
    - ComexExtractor com fixtures CSV (Exp/Imp calculados com PTAX=5.22)
    - IBGEExtractor mockado → VAB = 124.859 milhões R$
    - PTAXExtractor mockado → PTAX = 5.22 R$/USD
    """
    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX_REF,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        return_value=_VAB_REF,
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                str(config_integracao),
                "--saida",
                str(saida_dir),
                "--formato",
                "pdf",
                "excel",
            ],
        ):
            result = run()

    assert result == 0

    # Verifica que ambos os arquivos foram gerados
    assert (saida_dir / "gap_icms_2022.pdf").exists()
    assert (saida_dir / "gap_icms_2022.xlsx").exists()

    # VRR calculado com dados do fixture:
    # ICMS (fixture parquet) ≈ 9.8 M
    # Exp (fixture CSV, PTAX=5.22) ≈ 1.3M USD × 5.22 / 1M ≈ 6.786 M
    # Imp (fixture CSV, PTAX=5.22) ≈ 0.7M USD × 5.22 / 1M ≈ 3.654 M
    # VRR = 9.8 / ((124859 - 6.786 + 3.654) × 0.18) — valor variável mas deve estar entre 0 e 1
    # Verificamos apenas que o pipeline completou com sucesso


# ---------------------------------------------------------------------------
# Teste 5: Idempotência — execuções repetidas produzem resultados idênticos
# ---------------------------------------------------------------------------


def test_idempotencia_execucoes_repetidas_produzem_resultado_identico(
    config_integracao, tmp_path
):
    """Execuções repetidas com os mesmos dados produzem resultados idênticos."""
    from gap_tributario.engine.vrr import MotorVRR
    from gap_tributario.models import DadosVRR, PeriodoCalculo

    dados = DadosVRR(
        periodo=PeriodoCalculo(ano=2022),
        icms_arrecadado=_ICMS_REF,
        vab=_VAB_REF,
        exportacoes_brl=_EXP_REF,
        importacoes_brl=_IMP_REF,
        aliquota_padrao=_ALIQUOTA_REF,
        ptax_media=_PTAX_REF,
    )

    motor = MotorVRR()

    # Executar múltiplas vezes
    resultados = [motor.calcular(dados) for _ in range(3)]

    # Todos os resultados devem ser idênticos
    vrrs = [r.vrr for r in resultados]
    gaps = [r.gap_absoluto for r in resultados]
    potenciais = [r.icms_potencial for r in resultados]

    assert all(v == vrrs[0] for v in vrrs), "VRR não é idêntico entre execuções"
    assert all(g == gaps[0] for g in gaps), "Gap não é idêntico entre execuções"
    assert all(p == potenciais[0] for p in potenciais), "Potencial não é idêntico entre execuções"

    # Verificar valor absoluto do VRR
    vrr_val = float(vrrs[0])
    assert 0 < vrr_val < 1.5, f"VRR {vrr_val} fora do range esperado"


# ---------------------------------------------------------------------------
# Teste 6: Pipeline end-to-end via subprocess com --ptax-manual e --vab-manual
# ---------------------------------------------------------------------------


def test_pipeline_subprocess_ptax_vab_manuais_exit_0(config_integracao, saida_dir):
    """Pipeline via subprocess com --ptax-manual e --vab-manual retorna exit(0).

    Usa as fixtures de Parquet/CSV locais e bypassa APIs via flags manuais.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "gap_tributario",
            "--periodo",
            "2022",
            "--ptax-manual",
            "5.22",
            "--vab-manual",
            "124859",
            "--config",
            str(config_integracao),
            "--saida",
            str(saida_dir),
            "--formato",
            "pdf",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Pipeline falhou com exit {result.returncode}.\n"
        f"stderr: {result.stderr}"
    )

    pdf_file = saida_dir / "gap_icms_2022.pdf"
    assert pdf_file.exists(), "Arquivo PDF não foi gerado via subprocess"
    # Verifica que o caminho do arquivo foi impresso no stdout
    assert "gap_icms_2022.pdf" in result.stdout
