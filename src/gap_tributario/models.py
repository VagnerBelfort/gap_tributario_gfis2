"""Modelos de dados (dataclasses) para a Calculadora de Gap Tributário.

Define os contratos de dados compartilhados por todos os módulos:
- PeriodoCalculo: representa um período de cálculo (trimestral ou anual)
- DadosVRR: dados de entrada consolidados para o cálculo VRR
- ResultadoGap: resultado do cálculo do gap tributário
- ConfigAliquota: configuração de alíquota por período
- AppConfig: configuração global da aplicação
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class PeriodoCalculo:
    """Representa um período de cálculo (trimestral ou anual)."""

    ano: int
    trimestre: Optional[int] = None  # None = anual, 1-4 = trimestre

    @property
    def is_anual(self) -> bool:
        """Retorna True se o período é anual."""
        return self.trimestre is None

    @property
    def label(self) -> str:
        """Retorna o label textual do período."""
        if self.is_anual:
            return str(self.ano)
        return f"{self.ano}-T{self.trimestre}"

    def __post_init__(self) -> None:
        if self.trimestre is not None and self.trimestre not in (1, 2, 3, 4):
            raise ValueError(f"Trimestre deve ser 1-4, recebeu: {self.trimestre}")
        if self.ano < 2010 or self.ano > 2030:
            raise ValueError(f"Ano fora do range válido (2010-2030): {self.ano}")

    @classmethod
    def from_string(cls, periodo_str: str) -> "PeriodoCalculo":
        """Cria um PeriodoCalculo a partir de string no formato 'YYYY' ou 'YYYY-TN'.

        Args:
            periodo_str: String no formato "2022" ou "2022-T1"

        Returns:
            PeriodoCalculo correspondente

        Raises:
            ValueError: Se o formato for inválido
        """
        periodo_str = periodo_str.strip().upper()

        if "-T" in periodo_str:
            parts = periodo_str.split("-T")
            if len(parts) != 2:
                raise ValueError(
                    f"Formato de período inválido: '{periodo_str}'. Use YYYY ou YYYY-TN (N=1-4)"
                )
            try:
                ano = int(parts[0])
                trimestre = int(parts[1])
            except ValueError as exc:
                raise ValueError(
                    f"Formato de período inválido: '{periodo_str}'. Use YYYY ou YYYY-TN (N=1-4)"
                ) from exc
            return cls(ano=ano, trimestre=trimestre)
        else:
            try:
                ano = int(periodo_str)
            except ValueError as exc:
                raise ValueError(
                    f"Formato de período inválido: '{periodo_str}'. Use YYYY ou YYYY-TN (N=1-4)"
                ) from exc
            return cls(ano=ano)


@dataclass(frozen=True)
class DadosVRR:
    """Dados de entrada consolidados para o cálculo VRR."""

    periodo: PeriodoCalculo
    icms_arrecadado: Decimal  # Em milhões R$ — soma val_icms_normal + val_icms_imp + val_icms_st
    vab: Decimal  # Valor Adicionado Bruto em milhões R$ (IBGE SIDRA 5938)
    exportacoes_brl: Decimal  # Exportações FOB em milhões R$ (MDIC × PTAX)
    importacoes_brl: Decimal  # Importações FOB em milhões R$ (MDIC × PTAX)
    aliquota_padrao: Decimal  # Alíquota modal ICMS-MA (0.18 ou 0.20)
    ptax_media: Decimal  # Cotação média USD/BRL do período (BCB PTAX)

    def __post_init__(self) -> None:
        if self.aliquota_padrao <= Decimal("0") or self.aliquota_padrao >= Decimal("1"):
            raise ValueError(f"Alíquota deve estar entre 0 e 1: {self.aliquota_padrao}")
        if self.icms_arrecadado < Decimal("0"):
            raise ValueError(f"ICMS arrecadado não pode ser negativo: {self.icms_arrecadado}")


@dataclass(frozen=True)
class ResultadoGap:
    """Resultado do cálculo do Gap Tributário."""

    periodo: PeriodoCalculo
    icms_arrecadado: Decimal  # R$ milhões
    icms_potencial: Decimal  # R$ milhões = (VAB - Exp + Imp) × Alíquota
    vrr: Decimal  # Ratio 0-1 (quanto mais próximo de 1, melhor)
    gap_absoluto: Decimal  # R$ milhões = Potencial - Arrecadado
    gap_percentual: Decimal  # % = (Gap / Potencial) × 100
    vab: Decimal  # Para referência no relatório
    exportacoes_brl: Decimal  # Para referência no relatório
    importacoes_brl: Decimal  # Para referência no relatório
    aliquota_padrao: Decimal  # Para referência no relatório
    ptax_media: Decimal  # Para referência no relatório


@dataclass
class ConfigAliquota:
    """Configuração de alíquota por período."""

    ano_inicio: int
    ano_fim: Optional[int]  # None = vigente
    aliquota: Decimal
    legislacao: str  # Referência legal


@dataclass
class AppConfig:
    """Configuração global da aplicação."""

    aliquotas: List[ConfigAliquota]
    parquet_base_path: Path
    mdic_base_path: Path
    oracle_dsn: Optional[str]  # None se Siscomex desabilitado
    oracle_user: Optional[str]
    oracle_password: Optional[str]
    output_path: Path

    def get_aliquota(self, periodo: PeriodoCalculo) -> Decimal:
        """Retorna a alíquota vigente para o período.

        Args:
            periodo: Período de cálculo

        Returns:
            Alíquota como Decimal

        Raises:
            ValueError: Se nenhuma alíquota estiver configurada para o período
        """
        for aliq in self.aliquotas:
            if aliq.ano_inicio <= periodo.ano:
                if aliq.ano_fim is None or periodo.ano <= aliq.ano_fim:
                    return aliq.aliquota
        raise ValueError(f"Nenhuma alíquota configurada para {periodo.ano}")
