"""Testes da interface CLI e orquestração do pipeline.

Cobre:
- Testes básicos de --version, --help e sem argumentos (subprocess)
- Validação de --periodo e --config (subprocess)
- Pipeline completo com extractors mockados (unittest.mock.patch)
- Códigos de saída para cada tipo de falha
- Overrides --ptax-manual e --vab-manual
- Flag --siscomex
"""

from __future__ import annotations

import logging
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from gap_tributario.cli import run

# Valores de referência MA 2022 para testes de pipeline
_PTAX = Decimal("5.22")
_VAB = Decimal("124859")
_ICMS = Decimal("10917")
_EXP = Decimal("29754")
_IMP = Decimal("21924")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def saida(tmp_path: Path) -> Path:
    """Diretório de saída temporário."""
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture
def config_path(config_yaml_path: Path) -> str:
    """Retorna o caminho do arquivo de configuração de teste como string."""
    return str(config_yaml_path)


def _mock_extractors(
    ptax: Decimal = _PTAX,
    vab: Decimal = _VAB,
    icms: Decimal = _ICMS,
    exp: Decimal = _EXP,
    imp: Decimal = _IMP,
):
    """Contexto com todos os extractors mockados retornando valores de referência."""
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(
        patch(
            "gap_tributario.extractors.ptax.PTAXExtractor.extract",
            return_value=ptax,
        )
    )
    stack.enter_context(
        patch(
            "gap_tributario.extractors.ibge.IBGEExtractor.extract",
            return_value=vab,
        )
    )
    stack.enter_context(
        patch(
            "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
            return_value=icms,
        )
    )
    stack.enter_context(
        patch(
            "gap_tributario.extractors.comex.ComexExtractor.extract",
            return_value=(exp, imp),
        )
    )
    return stack


# ---------------------------------------------------------------------------
# Testes básicos (subprocess) — já existentes
# ---------------------------------------------------------------------------


def test_version_command():
    """Verifica que --version retorna a versão sem erro."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "0.1.0" in result.stdout


def test_help_command():
    """Verifica que --help retorna ajuda sem erro."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Gap Tributário" in result.stdout


def test_no_args_shows_help():
    """Verifica que execução sem argumentos mostra help."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# Validação de --periodo (subprocess) — exit code 4
# ---------------------------------------------------------------------------


def test_periodo_formato_invalido_t5_retorna_exit_4():
    """--periodo 2022-T5 é inválido → exit(4)."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--periodo", "2022-T5"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 4


def test_periodo_formato_texto_invalido_retorna_exit_4():
    """--periodo abc é inválido → exit(4)."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--periodo", "abc"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 4


def test_periodo_ano_fora_do_range_1990_retorna_exit_4():
    """--periodo 1990 está fora do range 2010-2030 → exit(4)."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--periodo", "1990"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 4


def test_periodo_ano_fora_do_range_2031_retorna_exit_4():
    """--periodo 2031 está fora do range 2010-2030 → exit(4)."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--periodo", "2031"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 4


# ---------------------------------------------------------------------------
# Validação de --config (subprocess) — exit code 3
# ---------------------------------------------------------------------------


def test_config_arquivo_inexistente_retorna_exit_3():
    """--config aponta para arquivo inexistente → exit(3)."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "gap_tributario",
            "--periodo",
            "2022",
            "--config",
            "/caminho/inexistente/config.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 3


# ---------------------------------------------------------------------------
# Testes de pipeline com extractors mockados
# ---------------------------------------------------------------------------


def test_ptax_manual_bypassa_ptax_extractor(config_path, saida):
    """--ptax-manual bypassa a chamada à PTAXExtractor.extract()."""
    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract"
    ) as mock_ptax_extract, patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        return_value=_VAB,
    ), patch(
        "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
        return_value=_ICMS,
    ), patch(
        "gap_tributario.extractors.comex.ComexExtractor.extract",
        return_value=(_EXP, _IMP),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--ptax-manual",
                "5.22",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    mock_ptax_extract.assert_not_called()


def test_vab_manual_bypassa_ibge_extractor(config_path, saida):
    """--vab-manual bypassa a chamada à IBGEExtractor.extract()."""
    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract"
    ) as mock_ibge_extract, patch(
        "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
        return_value=_ICMS,
    ), patch(
        "gap_tributario.extractors.comex.ComexExtractor.extract",
        return_value=(_EXP, _IMP),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--vab-manual",
                "124859",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    mock_ibge_extract.assert_not_called()


def test_pipeline_formato_pdf_gera_arquivo_correto(config_path, saida):
    """Pipeline completo com --formato pdf gera arquivo PDF com nome correto."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    assert (saida / "gap_icms_2022.pdf").exists()
    assert not (saida / "gap_icms_2022.xlsx").exists()


def test_pipeline_formato_excel_gera_arquivo_correto(config_path, saida):
    """Pipeline completo com --formato excel gera arquivo Excel com nome correto."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "excel",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    assert (saida / "gap_icms_2022.xlsx").exists()
    assert not (saida / "gap_icms_2022.pdf").exists()


def test_pipeline_formato_ambos_gera_pdf_e_excel(config_path, saida):
    """Pipeline completo com --formato pdf excel gera ambos os arquivos."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "pdf",
                "excel",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    assert (saida / "gap_icms_2022.pdf").exists()
    assert (saida / "gap_icms_2022.xlsx").exists()


def test_pipeline_formato_default_gera_pdf_e_excel(config_path, saida):
    """Pipeline sem --formato usa default (pdf excel) e gera ambos os arquivos."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    assert (saida / "gap_icms_2022.pdf").exists()
    assert (saida / "gap_icms_2022.xlsx").exists()


def test_pipeline_periodo_trimestral_t1(config_path, saida):
    """Pipeline com período trimestral 2022-T1 gera arquivos com label correto."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022-T1",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    assert (saida / "gap_icms_2022-T1.pdf").exists()


# ---------------------------------------------------------------------------
# Testes de erros de extração — exit code 2
# ---------------------------------------------------------------------------


def test_extraction_error_ptax_retorna_exit_2(config_path, saida):
    """ExtractionError em PTAXExtractor → exit(2)."""
    from gap_tributario.extractors.base import ExtractionError

    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        side_effect=ExtractionError("BCB indisponível"),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 2


def test_extraction_error_ibge_retorna_exit_2(config_path, saida):
    """ExtractionError em IBGEExtractor → exit(2)."""
    from gap_tributario.extractors.base import ExtractionError

    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        side_effect=ExtractionError("IBGE indisponível"),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 2


def test_extraction_error_arrecadacao_retorna_exit_2(config_path, saida):
    """ExtractionError em ArrecadacaoExtractor → exit(2)."""
    from gap_tributario.extractors.base import ExtractionError

    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        return_value=_VAB,
    ), patch(
        "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
        side_effect=ExtractionError("Parquet não encontrado"),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 2


def test_extraction_error_comex_retorna_exit_2(config_path, saida):
    """ExtractionError em ComexExtractor → exit(2)."""
    from gap_tributario.extractors.base import ExtractionError

    with patch(
        "gap_tributario.extractors.ptax.PTAXExtractor.extract",
        return_value=_PTAX,
    ), patch(
        "gap_tributario.extractors.ibge.IBGEExtractor.extract",
        return_value=_VAB,
    ), patch(
        "gap_tributario.extractors.arrecadacao.ArrecadacaoExtractor.extract",
        return_value=_ICMS,
    ), patch(
        "gap_tributario.extractors.comex.ComexExtractor.extract",
        side_effect=ExtractionError("CSVs MDIC não encontrados"),
    ):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 2


# ---------------------------------------------------------------------------
# Testes de erros de validação/cálculo — exit code 1
# ---------------------------------------------------------------------------


def test_valor_error_motor_vrr_base_zero_retorna_exit_1(config_path, saida):
    """ValueError em MotorVRR (base de cálculo zero) → exit(1)."""
    # VAB = 0, Exp = 0, Imp = 0 → base = 0 → ValueError
    with _mock_extractors(vab=Decimal("0"), exp=Decimal("0"), imp=Decimal("0")):
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 1


# ---------------------------------------------------------------------------
# Testes de erro de escrita de relatório — exit code 2
# ---------------------------------------------------------------------------


def test_os_error_geracao_relatorio_retorna_exit_2(config_path, saida):
    """OSError na geração do relatório → exit(2)."""
    with _mock_extractors():
        with patch(
            "gap_tributario.report.pdf.PDFReport.gerar",
            side_effect=OSError("Permissão negada"),
        ), patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 2


# ---------------------------------------------------------------------------
# Testes de flag --siscomex
# ---------------------------------------------------------------------------


def test_siscomex_sem_oracle_dsn_nao_aborta(config_path, saida):
    """--siscomex sem oracle_dsn configurado não aborta — apenas avisa e continua."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--siscomex",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    # oracle_dsn está vazio no config de teste → warning silencioso, continua
    assert result == 0


# ---------------------------------------------------------------------------
# Testes de --verbose
# ---------------------------------------------------------------------------


def test_verbose_flag_nao_causa_erro(config_path, saida):
    """--verbose não deve causar erros e retorna exit(0)."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--verbose",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0


def test_verbose_configura_nivel_debug(config_path, saida):
    """--verbose deve configurar logging no nível DEBUG."""
    with _mock_extractors():
        with patch("gap_tributario.cli.logging.basicConfig") as mock_basic_config:
            with patch(
                "sys.argv",
                [
                    "gap-tributario",
                    "--periodo",
                    "2022",
                    "--verbose",
                    "--formato",
                    "pdf",
                    "--config",
                    config_path,
                    "--saida",
                    str(saida),
                ],
            ):
                run()

    # Verifica que basicConfig foi chamado com level=DEBUG
    call_args = mock_basic_config.call_args
    assert call_args is not None
    assert call_args.kwargs.get("level") == logging.DEBUG or call_args.args[0] == logging.DEBUG


def test_sem_verbose_configura_nivel_info(config_path, saida):
    """Sem --verbose, logging deve ser configurado no nível INFO."""
    with _mock_extractors():
        with patch("gap_tributario.cli.logging.basicConfig") as mock_basic_config:
            with patch(
                "sys.argv",
                [
                    "gap-tributario",
                    "--periodo",
                    "2022",
                    "--formato",
                    "pdf",
                    "--config",
                    config_path,
                    "--saida",
                    str(saida),
                ],
            ):
                run()

    call_args = mock_basic_config.call_args
    assert call_args is not None
    assert call_args.kwargs.get("level") == logging.INFO or call_args.args[0] == logging.INFO


# ---------------------------------------------------------------------------
# Testes de saída stdout
# ---------------------------------------------------------------------------


def test_pipeline_imprime_caminhos_dos_arquivos(config_path, saida, capsys):
    """Pipeline deve imprimir os caminhos dos arquivos gerados no stdout."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "pdf",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    captured = capsys.readouterr()
    assert "gap_icms_2022.pdf" in captured.out


def test_pipeline_ambos_formatos_imprime_dois_caminhos(config_path, saida, capsys):
    """Pipeline com pdf e excel deve imprimir dois caminhos no stdout."""
    with _mock_extractors():
        with patch(
            "sys.argv",
            [
                "gap-tributario",
                "--periodo",
                "2022",
                "--formato",
                "pdf",
                "excel",
                "--config",
                config_path,
                "--saida",
                str(saida),
            ],
        ):
            result = run()

    assert result == 0
    captured = capsys.readouterr()
    assert "gap_icms_2022.pdf" in captured.out
    assert "gap_icms_2022.xlsx" in captured.out
