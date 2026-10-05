"""Testes da exportação para a camada ouro (gfis2_ouro.g_gap_*).

O painel da SEFAZ lê essas tabelas: colunas, ordem e valores categóricos são os
da carga de 31/08/2026 e não mudam. Os cabeçalhos abaixo são transcritos do DDL
de produção, não derivados do código.
"""

from __future__ import annotations

import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.engine.gap_decomposition import decompor_gap
from gap_tributario.engine.vrr import MotorVRR
from gap_tributario.models import (
    DadosVRR,
    PeriodoCalculo,
    Proveniencia,
    RenunciaFiscal,
)
from gap_tributario.report.ouro import CalculoPeriodo, exportar_ouro

COLUNAS_RESULTADO = [
    "ano", "tipo_periodo", "nro_trimestre", "vab", "exportacoes", "importacoes",
    "base_calculo", "aliquota", "ptax_media", "icms_potencial", "icms_arrecadado",
    "vrr", "gap_absoluto", "gap_percentual", "flg_parcial", "flg_estimado",
    "fonte_vab", "fonte_icms", "fonte_imp", "fonte_exp", "dt_calculo",
    "versao_engine", "id_execucao",
]

DT_CALCULO = datetime(2026, 10, 1, 14, 30, 0)


def _ler(caminho: Path) -> list[dict]:
    with caminho.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _cabecalho(caminho: Path) -> list[str]:
    with caminho.open(encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def _calculo(
    ano: int | str,
    vab: str,
    exp: str,
    imp: str,
    icms: str,
    aliq: str,
    renuncia: RenunciaFiscal | None = None,
    fontes: dict | None = None,
    ordem_cascata: dict | None = None,
    importacoes_por_fonte: dict | None = None,
) -> CalculoPeriodo:
    dados = DadosVRR(
        periodo=PeriodoCalculo.from_string(str(ano)),
        icms_arrecadado=Decimal(icms),
        vab=Decimal(vab),
        exportacoes_brl=Decimal(exp),
        importacoes_brl=Decimal(imp),
        aliquota_padrao=Decimal(aliq),
        ptax_media=Decimal("5.1655"),
    )
    resultado = MotorVRR().calcular(dados)
    return CalculoPeriodo(
        resultado=resultado,
        decomposicao=decompor_gap(resultado, renuncia) if renuncia else None,
        proveniencias=[
            Proveniencia("VAB", "IMESC", "PIB Trimestral, Tabela 15", "2026-10-01"),
            Proveniencia("ICMS Arrecadado", "GFIS2", "g_arrecadacao", "2026-10-01"),
            Proveniencia("Exportações", "MDIC ComEx", "ComexStat", "2026-10-01"),
            Proveniencia("Importações", "Siscomex (SEFAZ-MA)", "APL_SISCOMEX", "2026-10-01",
                         "Valor aduaneiro (CIF) por domicílio fiscal."),
            Proveniencia("Câmbio (PTAX)", "BCB", "API Olinda PTAX", "2026-10-01"),
        ] + ([Proveniencia("Renúncia Fiscal (ICMS)", "AMF Tabela 7 (LDO/MA)",
                           "Anexo de Metas Fiscais, Tabela 7", "2026-10-01")]
             if renuncia else []),
        fontes=fontes or {"vab": "imesc", "icms": "gfis2", "imp": "siscomex", "exp": "mdic",
                          "ptax": "bcb_olinda"},
        ordem_cascata=ordem_cascata or {"vab": 1, "icms": 1, "imp": 1, "exp": 1, "ptax": 1},
        importacoes_por_fonte=importacoes_por_fonte or {"DI": Decimal(imp)},
        legislacao_aliquota="Lei Estadual vigente até 2022",
    )


@pytest.fixture
def calculo_2022() -> CalculoPeriodo:
    """Entradas do golden de fórmula MA 2022 (ver docstring de engine/vrr.py)."""
    return _calculo(2022, vab="124859", exp="29754", imp="21924", icms="10917", aliq="0.18")


def test_resultado_tem_as_colunas_do_ddl_de_producao_e_os_valores_do_golden(
    tmp_path: Path, calculo_2022: CalculoPeriodo
):
    arquivos = exportar_ouro([calculo_2022], tmp_path, DT_CALCULO)

    caminho = arquivos["g_gap_resultado"]
    assert _cabecalho(caminho) == COLUNAS_RESULTADO
    [linha] = _ler(caminho)
    assert linha["ano"] == "2022"
    assert linha["tipo_periodo"] == "A"
    assert linha["nro_trimestre"] == "0"
    assert linha["vab"] == "124859.00"
    assert linha["base_calculo"] == "117029.00"
    assert linha["aliquota"] == "0.1800"
    assert linha["ptax_media"] == "5.1655"
    assert linha["icms_potencial"] == "21065.22"
    assert linha["icms_arrecadado"] == "10917.00"
    assert linha["vrr"].startswith("0.5182")
    assert linha["gap_absoluto"] == "10148.22"
    assert linha["flg_parcial"] == "false"
    assert linha["flg_estimado"] == "false"
    assert (linha["fonte_vab"], linha["fonte_icms"], linha["fonte_imp"], linha["fonte_exp"]) == (
        "imesc", "gfis2", "siscomex", "mdic",
    )
    assert linha["dt_calculo"] == "2026-10-01 14:30:00"
    assert linha["versao_engine"] == "gap_tributario-cli-2026-10-01"
    assert linha["id_execucao"] == "CARGA_CLI_2026-10-01"


COLUNAS_DECOMPOSICAO = [
    "ano", "componente", "valor", "pct_do_gap", "vintage_ldo",
    "flg_compliance_negativo", "dt_calculo", "id_execucao",
]

# AMF Tabela 7, LDO-2022, ano 2022 — src/gap_tributario/data/amf_renuncia_icms_ma.csv
RENUNCIA_2022 = RenunciaFiscal(
    ano=2022,
    total=Decimal("2182.131338"),
    por_modalidade={
        "Crédito Presumido": Decimal("1268.007848"),
        "Isenção": Decimal("520.062369"),
        "Redução de Base de Cálculo": Decimal("394.061121"),
    },
    vintage="LDO-2022",
)


def test_decomposicao_usa_as_modalidades_reais_da_amf_e_omite_ano_sem_renuncia(tmp_path: Path):
    com_amf = _calculo(2022, vab="124859", exp="29754", imp="21924", icms="10917",
                       aliq="0.18", renuncia=RENUNCIA_2022)
    sem_amf = _calculo(2020, vab="95497.34", exp="17387.76", imp="11504.37",
                       icms="8195.78", aliq="0.18")

    arquivos = exportar_ouro([sem_amf, com_amf], tmp_path, DT_CALCULO)

    caminho = arquivos["g_gap_decomposicao"]
    assert _cabecalho(caminho) == COLUNAS_DECOMPOSICAO
    linhas = _ler(caminho)
    assert {linha["ano"] for linha in linhas} == {"2022"}
    valores = {linha["componente"]: linha["valor"] for linha in linhas}
    assert valores == {
        "GAP_TOTAL": "10148.22",
        "POLICY": "2182.13",
        "COMPLIANCE": "7966.09",
        "MOD_CREDITO_PRESUMIDO": "1268.01",
        "MOD_ISENCAO": "520.06",
        "MOD_REDUCAO_BASE": "394.06",
    }
    pcts = {linha["componente"]: linha["pct_do_gap"] for linha in linhas}
    assert pcts["GAP_TOTAL"] == "100.0000"
    assert pcts["MOD_ISENCAO"] == "5.1247"  # 520,062369 / 10.148,22
    assert {linha["vintage_ldo"] for linha in linhas} == {"LDO-2022"}
    assert {linha["flg_compliance_negativo"] for linha in linhas} == {"false"}
    assert {linha["id_execucao"] for linha in linhas} == {"CARGA_CLI_2026-10-01"}


COLUNAS_PROVENIENCIA = [
    "ano", "tipo_periodo", "nro_trimestre", "variavel", "valor", "origem", "fonte",
    "dt_extracao", "observacoes", "fonte_vencedora", "ordem_cascata", "dt_calculo",
    "id_execucao",
]

# Nomes gravados na carga de 31/08/2026 — o painel depende deles.
VARIAVEIS_OURO = [
    "VAB", "ICMS Arrecadado", "Exportações", "Importações", "PTAX média",
    "Alíquota modal", "Renúncia fiscal (ICMS)",
]


def test_proveniencia_traz_as_sete_variaveis_e_separa_di_de_duimp(tmp_path: Path):
    calculo_2025 = _calculo(
        2025, vab="156260", exp="28055.73", imp="27828.41", icms="15761.73", aliq="0.20",
        importacoes_por_fonte={"DI": Decimal("27208.44"), "DUIMP": Decimal("619.97")},
    )

    arquivos = exportar_ouro([calculo_2025], tmp_path, DT_CALCULO)

    caminho = arquivos["g_gap_proveniencia"]
    assert _cabecalho(caminho) == COLUNAS_PROVENIENCIA
    linhas = {linha["variavel"]: linha for linha in _ler(caminho)}
    assert list(linhas) == VARIAVEIS_OURO
    imp = linhas["Importações"]
    assert imp["valor"] == "27828.4100"
    assert imp["observacoes"].startswith("DI R$ 27.208,44 mi + DUIMP R$ 619,97 mi.")
    assert "Valor aduaneiro (CIF) por domicílio fiscal." in imp["observacoes"]
    assert (imp["fonte_vencedora"], imp["ordem_cascata"]) == ("siscomex", "1")
    assert (linhas["PTAX média"]["valor"], linhas["PTAX média"]["fonte_vencedora"]) == (
        "5.1655", "bcb_olinda",
    )
    assert linhas["Alíquota modal"]["valor"] == "0.2000"
    assert linhas["Alíquota modal"]["fonte_vencedora"] == "config"
    assert linhas["VAB"]["dt_extracao"] == "2026-10-01"


def test_ano_sem_amf_grava_renuncia_nula_e_a_posicao_que_o_cli_deu_na_cascata(tmp_path: Path):
    calculo_2020 = _calculo(
        2020, vab="95497.34", exp="17387.76", imp="11504.37", icms="8195.78", aliq="0.18",
        fontes={"vab": "ibge_sidra", "icms": "gfis2", "imp": "siscomex", "exp": "mdic",
                "ptax": "bcb_olinda"},
        ordem_cascata={"vab": 2, "icms": 1, "imp": 1, "exp": 1, "ptax": 1},
    )

    arquivos = exportar_ouro([calculo_2020], tmp_path, DT_CALCULO)

    linhas = {linha["variavel"]: linha for linha in _ler(arquivos["g_gap_proveniencia"])}
    renuncia = linhas["Renúncia fiscal (ICMS)"]
    assert renuncia["valor"] == ""
    assert renuncia["fonte_vencedora"] == ""
    assert (linhas["VAB"]["fonte_vencedora"], linhas["VAB"]["ordem_cascata"]) == (
        "ibge_sidra", "2",
    )


def test_origem_e_fonte_repetem_os_textos_da_carga_de_agosto(tmp_path: Path):
    """O painel exibe esses textos; a carga de 31/08/2026 é a referência."""
    calculo = _calculo(2022, vab="124859", exp="29754", imp="21924", icms="10917",
                       aliq="0.18", renuncia=RENUNCIA_2022)

    arquivos = exportar_ouro([calculo], tmp_path, DT_CALCULO)

    textos = {
        linha["variavel"]: (linha["origem"], linha["fonte"])
        for linha in _ler(arquivos["g_gap_proveniencia"])
    }
    assert textos == {
        "VAB": ("IMESC / IBGE SIDRA 5938",
                "Relatório PIB Trimestral (IMESC); fallback IBGE Contas Regionais"),
        "ICMS Arrecadado": ("GFIS2 (gfis2_ouro.g_arrecadacao)",
                            "Soma de todas as parcelas de ICMS (normal, importação, ST saída, "
                            "ST entrada, dívida ativa, TVI, FCP, FDI, IDH, fruição)"),
        "Exportações": ("MDIC ComexStat", "EXP_2022.csv — FOB USD × PTAX"),
        "Importações": ("Siscomex (APL_SISCOMEX / SEFAZ-MA)",
                        "TDS_UF_IMPORTADOR — domicílio fiscal, valor aduaneiro (CIF)"),
        "PTAX média": ("BCB API Olinda", "Média de cotacaoVenda dos dias úteis do período"),
        "Alíquota modal": ("config/aliquotas.yaml", "Lei Estadual vigente até 2022"),
        "Renúncia fiscal (ICMS)": ("AMF Tabela 7 (LDO/MA)", "LDO-2022"),
    }


def test_trimestre_sai_com_tipo_t_sem_decomposicao_e_com_renuncia_anual_explicada(tmp_path: Path):
    """A renúncia da AMF é anual: o trimestre não tem decomposição nem valor de renúncia."""
    trimestre = _calculo("2022-T2", vab="31000", exp="7400", imp="9900", icms="2871.4",
                         aliq="0.18")

    arquivos = exportar_ouro([trimestre], tmp_path, DT_CALCULO)

    [linha] = _ler(arquivos["g_gap_resultado"])
    assert (linha["ano"], linha["tipo_periodo"], linha["nro_trimestre"]) == ("2022", "T", "2")
    assert linha["icms_potencial"] == "6030.00"  # (31.000 − 7.400 + 9.900) × 0,18
    assert _ler(arquivos["g_gap_decomposicao"]) == []
    proveniencia = _ler(arquivos["g_gap_proveniencia"])
    assert [p["variavel"] for p in proveniencia] == VARIAVEIS_OURO
    assert {(p["tipo_periodo"], p["nro_trimestre"]) for p in proveniencia} == {("T", "2")}
    renuncia = proveniencia[-1]
    assert renuncia["valor"] == ""
    assert "anual" in renuncia["fonte"]
