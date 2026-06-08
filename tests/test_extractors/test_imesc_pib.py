"""Testes do extrator IMESC PIB Trimestral (VAB do Maranhão).

Testa o ImescPibExtractor, que lê os valores correntes (nominais) transcritos da
Tabela 15 do Relatório Especializado do PIB Trimestral do Maranhão (IMESC) e
retorna o VAB do MA por período:

- Período anual: VAB = soma dos 4 trimestres do ano.
- Período trimestral: VAB = valor do trimestre direto (sem rateio).
- Ano fora de cobertura: levanta ExtractionError (sinaliza fallback p/ SIDRA).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.extractors.imesc_pib import ImescPibExtractor
from gap_tributario.models import PeriodoCalculo


def test_vab_anual_2022_soma_quatro_trimestres(periodo_2022_anual: PeriodoCalculo) -> None:
    """VAB anual de 2022 é a soma dos 4 trimestres = 124.859 (âncora do golden)."""
    vab = ImescPibExtractor().extract(periodo_2022_anual)
    assert vab == Decimal("124859")


def test_vab_trimestral_direto_sem_rateio(periodo_2022_t1: PeriodoCalculo) -> None:
    """VAB trimestral é o valor do próprio trimestre, sem rateio nem divisão por 4."""
    vab = ImescPibExtractor().extract(periodo_2022_t1)
    assert vab == Decimal("25871")


def test_vab_anual_2024_ano_antes_impossivel_por_lag() -> None:
    """2024 (antes impossível pelo lag do IBGE) agora calcula: soma dos 4 trimestres."""
    vab = ImescPibExtractor().extract(PeriodoCalculo(ano=2024))
    assert vab == Decimal("144220")  # 32053 + 38289 + 36410 + 37468


def test_ano_fora_de_cobertura_levanta_extraction_error() -> None:
    """Ano anterior à cobertura (<2021) sinaliza fallback via ExtractionError."""
    with pytest.raises(ExtractionError):
        ImescPibExtractor().extract(PeriodoCalculo(ano=2019))


def test_trimestre_fora_de_cobertura_levanta_extraction_error() -> None:
    """Trimestre fora da cobertura sinaliza fallback via ExtractionError."""
    with pytest.raises(ExtractionError):
        ImescPibExtractor().extract(PeriodoCalculo(ano=2026, trimestre=1))


def test_proveniencia_descreve_fonte_imesc() -> None:
    """proveniencia() carrega a origem IMESC, a variável VAB e a data injetada."""
    from gap_tributario.models import Proveniencia

    prov = ImescPibExtractor().proveniencia("2026-06-08")

    assert isinstance(prov, Proveniencia)
    assert prov.variavel == "VAB"
    assert "IMESC" in prov.origem
    assert "Tabela 15" in prov.fonte
    assert prov.data_extracao == "2026-06-08"
    assert "2021" in prov.observacoes  # caveat de cobertura ≥2021
