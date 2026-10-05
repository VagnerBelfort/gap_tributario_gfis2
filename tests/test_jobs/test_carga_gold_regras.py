"""Travas do job de carga da ouro (jobs/carga_gold_regras.py).

O job grava nas tabelas que o painel da SEFAZ lê. Estas regras decidem quando
ele aborta (estrutura ou valor categórico diferente do de produção) e quando só
avisa (ICMS do CSV afastado da g_arrecadacao do momento).
"""

from decimal import Decimal

import carga_gold_regras as regras

from gap_tributario.report import ouro


def test_schema_igual_ao_de_producao_nao_tem_diferenca():
    esperado = [("ano", "int"), ("vab", "decimal(18,2)")]
    assert regras.diferencas_colunas(esperado, list(esperado)) == []


def test_schema_aponta_coluna_ausente_tipo_trocado_e_ordem_diferente():
    esperado = [("ano", "int"), ("vab", "decimal(18,2)"), ("vrr", "decimal(10,6)")]

    assert regras.diferencas_colunas(esperado, [("ano", "int"), ("vab", "decimal(18,2)")]) != []
    assert regras.diferencas_colunas(
        esperado, [("ano", "int"), ("vab", "double"), ("vrr", "decimal(10,6)")]
    ) != []
    assert regras.diferencas_colunas(
        esperado, [("vab", "decimal(18,2)"), ("ano", "int"), ("vrr", "decimal(10,6)")]
    ) != []


def test_schema_da_carga_tem_as_colunas_que_o_cli_exporta():
    """O CSV do CLI e a tabela do Impala precisam falar a mesma língua."""
    assert [c for c, _ in regras.SCHEMA_OURO["g_gap_resultado"]] == ouro.COLUNAS_RESULTADO
    assert [c for c, _ in regras.SCHEMA_OURO["g_gap_decomposicao"]] == ouro.COLUNAS_DECOMPOSICAO
    assert [c for c, _ in regras.SCHEMA_OURO["g_gap_proveniencia"]] == ouro.COLUNAS_PROVENIENCIA


def test_valor_categorico_inedito_e_apontado_e_ausencia_de_valor_nao():
    producao = {"fonte_imp": {"siscomex"}, "fonte_vab": {"imesc", "ibge_sidra"}}

    assert regras.valores_ineditos({"fonte_imp": {"siscomex"}, "fonte_vab": {"imesc"}},
                                   producao) == []
    [problema] = regras.valores_ineditos(
        {"fonte_imp": {"siscomex", "siscomex_duimp"}, "fonte_vab": {"imesc"}}, producao
    )
    assert "siscomex_duimp" in problema


def test_icms_avisa_so_acima_de_meio_por_cento_e_quando_falta_o_ano():
    arrecadacao = {2024: Decimal("13954.29"), 2025: Decimal("15761.74")}
    csv = {
        2024: Decimal("13954.29") * Decimal("1.004"),  # 0,4%: dentro
        2025: Decimal("15761.74") * Decimal("1.006"),  # 0,6%: fora
        2026: Decimal("100"),  # sem arrecadação na g_arrecadacao
    }

    avisos = regras.avisos_divergencia("ICMS", csv, arrecadacao, Decimal("0.5"))

    assert len(avisos) == 2
    assert any("2025" in a for a in avisos)
    assert any("2026" in a for a in avisos)


def test_importacoes_avisam_qualquer_diferenca_de_centavo_contra_a_prata():
    """O CSV e a prata saem da mesma extração: só arredondamento é aceitável."""
    prata = {2025: Decimal("27828.41")}

    assert regras.avisos_divergencia("Importações", {2025: Decimal("27828.41")}, prata,
                                     regras.TOLERANCIA_IMPORTACOES_PCT) == []
    assert regras.avisos_divergencia("Importações", {2025: Decimal("27828.61")}, prata,
                                     regras.TOLERANCIA_IMPORTACOES_PCT) != []


def test_colunas_de_icms_sao_as_mesmas_do_extrator():
    from gap_tributario.extractors.arrecadacao import _COLUNAS_ICMS

    assert regras.COLUNAS_ICMS == _COLUNAS_ICMS


def test_trimestre_e_aceito_mesmo_com_a_producao_so_anual():
    """A carga de 31/08 só tinha anos; o painel foi construído com trimestres."""
    producao = {"tipo_periodo": {"A"}}

    assert regras.valores_ineditos({"tipo_periodo": {"A", "T"}}, producao) == []
    assert regras.valores_ineditos({"tipo_periodo": {"A", "X"}}, producao) != []
