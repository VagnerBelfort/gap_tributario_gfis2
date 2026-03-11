"""Fixtures compartilhadas para todos os testes.

Este arquivo define fixtures do pytest disponíveis em todos os módulos de teste.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.models import AppConfig, ConfigAliquota, PeriodoCalculo


@pytest.fixture
def periodo_2022_anual() -> PeriodoCalculo:
    """Retorna o período anual de referência MA 2022."""
    return PeriodoCalculo(ano=2022)


@pytest.fixture
def periodo_2022_t1() -> PeriodoCalculo:
    """Retorna o primeiro trimestre de 2022."""
    return PeriodoCalculo(ano=2022, trimestre=1)


@pytest.fixture
def periodo_2023_anual() -> PeriodoCalculo:
    """Retorna o período anual 2023 (alíquota 20%)."""
    return PeriodoCalculo(ano=2023)


@pytest.fixture
def config_test(tmp_path: Path) -> AppConfig:
    """Retorna uma AppConfig de teste com paths temporários."""
    return AppConfig(
        aliquotas=[
            ConfigAliquota(
                ano_inicio=2010,
                ano_fim=2022,
                aliquota=Decimal("0.18"),
                legislacao="Lei Estadual vigente até 2022",
            ),
            ConfigAliquota(
                ano_inicio=2023,
                ano_fim=None,
                aliquota=Decimal("0.20"),
                legislacao="Lei 11.867/2022 — alíquota modal 20%",
            ),
        ],
        parquet_base_path=tmp_path / "parquet",
        mdic_base_path=tmp_path / "mdic",
        oracle_dsn=None,
        oracle_user=None,
        oracle_password=None,
        output_path=tmp_path / "output",
    )


@pytest.fixture
def config_yaml_path(tmp_path: Path) -> Path:
    """Cria um arquivo aliquotas.yaml de teste e retorna seu path."""
    config_content = """aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Lei Estadual vigente até 2022"
  - ano_inicio: 2023
    ano_fim: null
    aliquota: 0.20
    legislacao: "Lei 11.867/2022 — alíquota modal 20%"

fontes:
  parquet_base_path: "./bases/g_arrecadacao/ouro/"
  mdic_base_path: "./mdic_comex/dados/"

oracle:
  dsn: ""
  user: ""
  password: ""
"""
    config_file = tmp_path / "aliquotas.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    return config_file
