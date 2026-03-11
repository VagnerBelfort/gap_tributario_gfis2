"""Testes unitários para src/gap_tributario/models.py.

Cobre todas as dataclasses: PeriodoCalculo, DadosVRR, ResultadoGap,
ConfigAliquota e AppConfig.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.models import (
    AppConfig,
    ConfigAliquota,
    DadosVRR,
    PeriodoCalculo,
    ResultadoGap,
)

# Python 3.11+ introduziu FrozenInstanceError; versões anteriores levantam TypeError
_FrozenInstanceError = getattr(dataclasses, "FrozenInstanceError", TypeError)


class TestPeriodoCalculo:
    """Testes para a dataclass PeriodoCalculo."""

    def test_periodo_anual_is_anual_true(self) -> None:
        """PeriodoCalculo anual deve ter is_anual=True."""
        periodo = PeriodoCalculo(ano=2022)
        assert periodo.is_anual is True

    def test_periodo_anual_label(self) -> None:
        """PeriodoCalculo anual deve ter label='2022'."""
        periodo = PeriodoCalculo(ano=2022)
        assert periodo.label == "2022"

    def test_periodo_anual_trimestre_none(self) -> None:
        """PeriodoCalculo anual deve ter trimestre=None."""
        periodo = PeriodoCalculo(ano=2022)
        assert periodo.trimestre is None

    def test_periodo_trimestral_is_anual_false(self) -> None:
        """PeriodoCalculo trimestral deve ter is_anual=False."""
        periodo = PeriodoCalculo(ano=2022, trimestre=1)
        assert periodo.is_anual is False

    def test_periodo_trimestral_label(self) -> None:
        """PeriodoCalculo trimestral deve ter label correto."""
        periodo = PeriodoCalculo(ano=2022, trimestre=1)
        assert periodo.label == "2022-T1"

    def test_todos_trimestres_validos(self) -> None:
        """Trimestres de 1 a 4 devem ser aceitos."""
        for t in range(1, 5):
            periodo = PeriodoCalculo(ano=2022, trimestre=t)
            assert periodo.trimestre == t
            assert periodo.label == f"2022-T{t}"

    def test_trimestre_zero_invalido(self) -> None:
        """Trimestre 0 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Trimestre deve ser 1-4"):
            PeriodoCalculo(ano=2022, trimestre=0)

    def test_trimestre_cinco_invalido(self) -> None:
        """Trimestre 5 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Trimestre deve ser 1-4"):
            PeriodoCalculo(ano=2022, trimestre=5)

    def test_trimestre_negativo_invalido(self) -> None:
        """Trimestre -1 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Trimestre deve ser 1-4"):
            PeriodoCalculo(ano=2022, trimestre=-1)

    def test_ano_abaixo_do_range(self) -> None:
        """Ano 2009 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Ano fora do range válido"):
            PeriodoCalculo(ano=2009)

    def test_ano_acima_do_range(self) -> None:
        """Ano 2031 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Ano fora do range válido"):
            PeriodoCalculo(ano=2031)

    def test_ano_limite_inferior_valido(self) -> None:
        """Ano 2010 deve ser aceito (limite inferior do range)."""
        periodo = PeriodoCalculo(ano=2010)
        assert periodo.ano == 2010

    def test_ano_limite_superior_valido(self) -> None:
        """Ano 2030 deve ser aceito (limite superior do range)."""
        periodo = PeriodoCalculo(ano=2030)
        assert periodo.ano == 2030

    def test_from_string_anual(self) -> None:
        """from_string('2022') deve retornar PeriodoCalculo(ano=2022) anual."""
        periodo = PeriodoCalculo.from_string("2022")
        assert periodo.ano == 2022
        assert periodo.trimestre is None
        assert periodo.is_anual is True

    def test_from_string_trimestre_1(self) -> None:
        """from_string('2022-T1') deve retornar trimestre=1."""
        periodo = PeriodoCalculo.from_string("2022-T1")
        assert periodo.ano == 2022
        assert periodo.trimestre == 1

    def test_from_string_trimestre_4(self) -> None:
        """from_string('2022-T4') deve retornar trimestre=4."""
        periodo = PeriodoCalculo.from_string("2022-T4")
        assert periodo.ano == 2022
        assert periodo.trimestre == 4

    def test_from_string_case_insensitive(self) -> None:
        """from_string deve aceitar case insensitivo: '2022-t1' == '2022-T1'."""
        periodo = PeriodoCalculo.from_string("2022-t1")
        assert periodo.ano == 2022
        assert periodo.trimestre == 1

    def test_from_string_invalido_abc(self) -> None:
        """from_string com 'abc' deve levantar ValueError."""
        with pytest.raises(ValueError, match="Formato de período inválido"):
            PeriodoCalculo.from_string("abc")

    def test_from_string_trimestre_cinco_invalido(self) -> None:
        """from_string com '2022-T5' deve levantar ValueError."""
        with pytest.raises(ValueError):
            PeriodoCalculo.from_string("2022-T5")

    def test_from_string_trimestre_zero_invalido(self) -> None:
        """from_string com '2022-T0' deve levantar ValueError."""
        with pytest.raises(ValueError):
            PeriodoCalculo.from_string("2022-T0")

    def test_from_string_trimestre_sem_numero(self) -> None:
        """from_string com '2022-T' deve levantar ValueError."""
        with pytest.raises(ValueError, match="Formato de período inválido"):
            PeriodoCalculo.from_string("2022-T")

    def test_periodo_frozen(self) -> None:
        """PeriodoCalculo é frozen=True — atribuição deve falhar."""
        periodo = PeriodoCalculo(ano=2022)
        with pytest.raises(_FrozenInstanceError):
            periodo.ano = 2023  # type: ignore[misc]

    def test_from_string_todos_trimestres(self) -> None:
        """from_string deve parsear corretamente todos os trimestres."""
        for t in range(1, 5):
            periodo = PeriodoCalculo.from_string(f"2022-T{t}")
            assert periodo.trimestre == t


class TestDadosVRR:
    """Testes para a dataclass DadosVRR."""

    def _make_dados(self, **overrides: object) -> DadosVRR:
        """Cria DadosVRR com valores de referência MA 2022."""
        defaults: dict = {
            "periodo": PeriodoCalculo(ano=2022),
            "icms_arrecadado": Decimal("10917"),
            "vab": Decimal("124859"),
            "exportacoes_brl": Decimal("29754"),
            "importacoes_brl": Decimal("21924"),
            "aliquota_padrao": Decimal("0.18"),
            "ptax_media": Decimal("5.22"),
        }
        defaults.update(overrides)
        return DadosVRR(**defaults)

    def test_dados_validos_ma_2022(self) -> None:
        """DadosVRR com dados de referência MA 2022 deve ser criado sem erros."""
        dados = self._make_dados()
        assert dados.aliquota_padrao == Decimal("0.18")
        assert dados.icms_arrecadado == Decimal("10917")

    def test_aliquota_zero_invalida(self) -> None:
        """Alíquota = 0 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Alíquota deve estar entre 0 e 1"):
            self._make_dados(aliquota_padrao=Decimal("0"))

    def test_aliquota_um_invalida(self) -> None:
        """Alíquota = 1 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Alíquota deve estar entre 0 e 1"):
            self._make_dados(aliquota_padrao=Decimal("1"))

    def test_aliquota_dezoito_valida(self) -> None:
        """Alíquota = 0.18 deve ser aceita."""
        dados = self._make_dados(aliquota_padrao=Decimal("0.18"))
        assert dados.aliquota_padrao == Decimal("0.18")

    def test_aliquota_vinte_valida(self) -> None:
        """Alíquota = 0.20 deve ser aceita."""
        dados = self._make_dados(aliquota_padrao=Decimal("0.20"))
        assert dados.aliquota_padrao == Decimal("0.20")

    def test_icms_negativo_invalido(self) -> None:
        """ICMS arrecadado negativo deve levantar ValueError."""
        with pytest.raises(ValueError, match="ICMS arrecadado não pode ser negativo"):
            self._make_dados(icms_arrecadado=Decimal("-1"))

    def test_icms_zero_valido(self) -> None:
        """ICMS arrecadado = 0 deve ser aceito (caso limite)."""
        dados = self._make_dados(icms_arrecadado=Decimal("0"))
        assert dados.icms_arrecadado == Decimal("0")

    def test_dados_frozen(self) -> None:
        """DadosVRR é frozen=True — atribuição deve falhar."""
        dados = self._make_dados()
        with pytest.raises(_FrozenInstanceError):
            dados.vab = Decimal("200000")  # type: ignore[misc]

    def test_todos_campos_decimal(self) -> None:
        """Todos os campos monetários devem ser do tipo Decimal."""
        dados = self._make_dados()
        assert isinstance(dados.icms_arrecadado, Decimal)
        assert isinstance(dados.vab, Decimal)
        assert isinstance(dados.exportacoes_brl, Decimal)
        assert isinstance(dados.importacoes_brl, Decimal)
        assert isinstance(dados.aliquota_padrao, Decimal)
        assert isinstance(dados.ptax_media, Decimal)

    def test_aliquota_muito_grande_invalida(self) -> None:
        """Alíquota = 1.5 deve levantar ValueError."""
        with pytest.raises(ValueError, match="Alíquota deve estar entre 0 e 1"):
            self._make_dados(aliquota_padrao=Decimal("1.5"))

    def test_aliquota_negativa_invalida(self) -> None:
        """Alíquota negativa deve levantar ValueError."""
        with pytest.raises(ValueError, match="Alíquota deve estar entre 0 e 1"):
            self._make_dados(aliquota_padrao=Decimal("-0.1"))


class TestResultadoGap:
    """Testes para a dataclass ResultadoGap."""

    def _make_resultado(self, **overrides: object) -> ResultadoGap:
        """Cria ResultadoGap com valores de referência MA 2022."""
        defaults: dict = {
            "periodo": PeriodoCalculo(ano=2022),
            "icms_arrecadado": Decimal("10917"),
            "icms_potencial": Decimal("21065.22"),
            "vrr": Decimal("0.5183"),
            "gap_absoluto": Decimal("10148.22"),
            "gap_percentual": Decimal("48.17"),
            "vab": Decimal("124859"),
            "exportacoes_brl": Decimal("29754"),
            "importacoes_brl": Decimal("21924"),
            "aliquota_padrao": Decimal("0.18"),
            "ptax_media": Decimal("5.22"),
        }
        defaults.update(overrides)
        return ResultadoGap(**defaults)

    def test_resultado_valido_ma_2022(self) -> None:
        """ResultadoGap com dados MA 2022 deve ser criado corretamente."""
        resultado = self._make_resultado()
        assert resultado.vrr == Decimal("0.5183")
        assert resultado.icms_potencial == Decimal("21065.22")
        assert resultado.gap_absoluto == Decimal("10148.22")

    def test_vrr_maior_que_um_permitido(self) -> None:
        """VRR > 1 deve ser permitido (indica dados atípicos, não é erro)."""
        resultado = self._make_resultado(vrr=Decimal("1.25"))
        assert resultado.vrr == Decimal("1.25")

    def test_resultado_frozen(self) -> None:
        """ResultadoGap é frozen=True — atribuição deve falhar."""
        resultado = self._make_resultado()
        with pytest.raises(_FrozenInstanceError):
            resultado.vrr = Decimal("0.9")  # type: ignore[misc]

    def test_resultado_tem_todos_os_campos_do_contrato(self) -> None:
        """ResultadoGap deve ter todos os 11 campos do contrato TechSpec."""
        resultado = self._make_resultado()
        assert hasattr(resultado, "periodo")
        assert hasattr(resultado, "icms_arrecadado")
        assert hasattr(resultado, "icms_potencial")
        assert hasattr(resultado, "vrr")
        assert hasattr(resultado, "gap_absoluto")
        assert hasattr(resultado, "gap_percentual")
        assert hasattr(resultado, "vab")
        assert hasattr(resultado, "exportacoes_brl")
        assert hasattr(resultado, "importacoes_brl")
        assert hasattr(resultado, "aliquota_padrao")
        assert hasattr(resultado, "ptax_media")

    def test_todos_campos_monetarios_decimal(self) -> None:
        """Todos os campos monetários do resultado devem ser Decimal."""
        resultado = self._make_resultado()
        assert isinstance(resultado.icms_arrecadado, Decimal)
        assert isinstance(resultado.icms_potencial, Decimal)
        assert isinstance(resultado.vrr, Decimal)
        assert isinstance(resultado.gap_absoluto, Decimal)
        assert isinstance(resultado.gap_percentual, Decimal)


class TestAppConfig:
    """Testes para AppConfig, especialmente get_aliquota()."""

    def _make_config(self) -> AppConfig:
        """Cria AppConfig de teste com alíquotas padrão (2010-2022 e 2023+)."""
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
                    legislacao="Lei 11.867/2022",
                ),
            ],
            parquet_base_path=Path("./bases"),
            mdic_base_path=Path("./mdic"),
            oracle_dsn=None,
            oracle_user=None,
            oracle_password=None,
            output_path=Path("./output"),
        )

    def test_get_aliquota_2022(self) -> None:
        """Período 2022 deve retornar Decimal('0.18')."""
        config = self._make_config()
        aliquota = config.get_aliquota(PeriodoCalculo(ano=2022))
        assert aliquota == Decimal("0.18")

    def test_get_aliquota_2023(self) -> None:
        """Período 2023 deve retornar Decimal('0.20')."""
        config = self._make_config()
        aliquota = config.get_aliquota(PeriodoCalculo(ano=2023))
        assert aliquota == Decimal("0.20")

    def test_get_aliquota_2010_inicio_do_range(self) -> None:
        """Período 2010 deve retornar Decimal('0.18') (início do range)."""
        config = self._make_config()
        aliquota = config.get_aliquota(PeriodoCalculo(ano=2010))
        assert aliquota == Decimal("0.18")

    def test_get_aliquota_periodo_sem_configuracao_levanta_value_error(self) -> None:
        """Período sem alíquota configurada deve levantar ValueError."""
        config = AppConfig(
            aliquotas=[
                ConfigAliquota(
                    ano_inicio=2015,
                    ano_fim=2022,
                    aliquota=Decimal("0.18"),
                    legislacao="Teste",
                ),
            ],
            parquet_base_path=Path("./bases"),
            mdic_base_path=Path("./mdic"),
            oracle_dsn=None,
            oracle_user=None,
            oracle_password=None,
            output_path=Path("./output"),
        )
        # Período 2010 não está coberto por config com range 2015-2022
        with pytest.raises(ValueError, match="Nenhuma alíquota configurada"):
            config.get_aliquota(PeriodoCalculo(ano=2010))

    def test_get_aliquota_futuro_usa_aliquota_vigente(self) -> None:
        """Período futuro deve usar alíquota vigente (ano_fim=None)."""
        config = self._make_config()
        aliquota = config.get_aliquota(PeriodoCalculo(ano=2026))
        assert aliquota == Decimal("0.20")

    def test_get_aliquota_retorna_decimal(self) -> None:
        """get_aliquota() deve retornar instância de Decimal, não float."""
        config = self._make_config()
        aliquota = config.get_aliquota(PeriodoCalculo(ano=2022))
        assert isinstance(aliquota, Decimal)

    def test_config_aliquota_campos(self) -> None:
        """ConfigAliquota deve ter todos os campos obrigatórios."""
        aliq = ConfigAliquota(
            ano_inicio=2010,
            ano_fim=2022,
            aliquota=Decimal("0.18"),
            legislacao="Lei Estadual",
        )
        assert aliq.ano_inicio == 2010
        assert aliq.ano_fim == 2022
        assert aliq.aliquota == Decimal("0.18")
        assert aliq.legislacao == "Lei Estadual"

    def test_config_aliquota_ano_fim_none(self) -> None:
        """ConfigAliquota com ano_fim=None representa alíquota vigente."""
        aliq = ConfigAliquota(
            ano_inicio=2023,
            ano_fim=None,
            aliquota=Decimal("0.20"),
            legislacao="Lei 11.867/2022",
        )
        assert aliq.ano_fim is None
