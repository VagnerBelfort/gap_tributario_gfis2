"""Testes da comparação entre leituras de fontes diferentes da mesma variável."""

from decimal import Decimal

import pytest

from gap_tributario.engine.comparacao import (
    descrever_faixa_observada,
    comparar_leituras,
)


class TestCompararLeituras:
    """A comparação Siscomex × MDIC é um controle de qualidade do dado."""

    def test_desvio_percentual_da_referencia_sobre_a_alternativa(self):
        """2022 real: Siscomex 39.703,79 contra MDIC 38.785,57 → +2,37%."""
        comparacao = comparar_leituras(
            referencia=Decimal("39703.79"), alternativa=Decimal("38785.57")
        )

        assert comparacao.desvio_pct == pytest.approx(Decimal("2.37"), abs=Decimal("0.01"))

    def test_desvio_negativo_quando_a_referencia_e_menor(self):
        """2011, fora da cobertura do Siscomex: -76% contra o MDIC."""
        comparacao = comparar_leituras(
            referencia=Decimal("2510"), alternativa=Decimal("10520")
        )

        assert comparacao.desvio_pct == pytest.approx(Decimal("-76.14"), abs=Decimal("0.01"))


class TestCorredorDeControle:
    """A tolerância do controle é mais larga que a faixa observada: o extremo
    medido (2022, +2,368%) precisa passar, e o arredondamento da exibição não
    pode transformar o limite em armadilha."""

    def test_extremo_inferior_observado_esta_dentro_do_corredor(self):
        comparacao = comparar_leituras(
            referencia=Decimal("39703.79"), alternativa=Decimal("38785.57")
        )

        assert comparacao.dentro_do_corredor is True

    def test_desvio_de_2011_esta_fora_do_corredor(self):
        """Ano sem cobertura do Siscomex: o controle precisa acusar."""
        comparacao = comparar_leituras(
            referencia=Decimal("2510"), alternativa=Decimal("10520")
        )

        assert comparacao.dentro_do_corredor is False

    def test_referencia_muito_acima_da_alternativa_tambem_e_fora(self):
        """Desvio de +40% não é assinatura de CIF sobre FOB."""
        comparacao = comparar_leituras(
            referencia=Decimal("14000"), alternativa=Decimal("10000")
        )

        assert comparacao.dentro_do_corredor is False


class TestCorredorNaoAplicavel:
    """A faixa foi medida em totais anuais. Aplicá-la a um trimestre produz
    alarme falso, então o controle se declara não avaliado."""

    def test_sem_corredor_o_desvio_continua_sendo_calculado(self):
        comparacao = comparar_leituras(
            referencia=Decimal("39703.79"),
            alternativa=Decimal("38785.57"),
            aplicar_corredor=False,
        )

        assert comparacao.desvio_pct == pytest.approx(Decimal("2.37"), abs=Decimal("0.01"))
        assert comparacao.dentro_do_corredor is None


class TestDescricaoDaFaixa:
    """O texto do relatório deriva das constantes, para não divergir delas."""

    def test_descricao_traz_a_faixa_observada_e_a_mediana(self):
        assert descrever_faixa_observada() == "+2,4% a +12,8% (mediana +6,8%)"


class TestAlternativaAusente:
    def test_alternativa_zerada_nao_produz_divisao_por_zero(self):
        with pytest.raises(ValueError, match="alternativa"):
            comparar_leituras(referencia=Decimal("39703.79"), alternativa=Decimal("0"))
