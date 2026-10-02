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
from gap_tributario.report.ouro import CalculoAnual, exportar_ouro

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
    ano: int,
    vab: str,
    exp: str,
    imp: str,
    icms: str,
    aliq: str,
    renuncia: RenunciaFiscal | None = None,
    fontes: dict | None = None,
    importacoes_por_fonte: dict | None = None,
) -> CalculoAnual:
    dados = DadosVRR(
        periodo=PeriodoCalculo(ano=ano),
        icms_arrecadado=Decimal(icms),
        vab=Decimal(vab),
        exportacoes_brl=Decimal(exp),
        importacoes_brl=Decimal(imp),
        aliquota_padrao=Decimal(aliq),
        ptax_media=Decimal("5.1655"),
    )
    resultado = MotorVRR().calcular(dados)
    return CalculoAnual(
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
        importacoes_por_fonte=importacoes_por_fonte or {"DI": Decimal(imp)},
        legislacao_aliquota="Lei Estadual vigente até 2022",
    )


@pytest.fixture
def calculo_2022() -> CalculoAnual:
    """Entradas do golden de fórmula MA 2022 (ver docstring de engine/vrr.py)."""
    return _calculo(2022, vab="124859", exp="29754", imp="21924", icms="10917", aliq="0.18")


def test_resultado_tem_as_colunas_do_ddl_de_producao_e_os_valores_do_golden(
    tmp_path: Path, calculo_2022: CalculoAnual
):
    arquivos = exportar_ouro([calculo_2022], tmp_path, DT_CALCULO, versao_engine="0.1.0")

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
    assert linha["versao_engine"] == "0.1.0"
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

    arquivos = exportar_ouro([sem_amf, com_amf], tmp_path, DT_CALCULO, versao_engine="0.1.0")

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

    arquivos = exportar_ouro([calculo_2025], tmp_path, DT_CALCULO, versao_engine="0.1.0")

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


def test_ano_sem_amf_grava_renuncia_nula_e_vab_do_sidra_em_segundo_na_cascata(tmp_path: Path):
    calculo_2020 = _calculo(
        2020, vab="95497.34", exp="17387.76", imp="11504.37", icms="8195.78", aliq="0.18",
        fontes={"vab": "ibge_sidra", "icms": "gfis2", "imp": "siscomex", "exp": "mdic",
                "ptax": "bcb_olinda"},
    )

    arquivos = exportar_ouro([calculo_2020], tmp_path, DT_CALCULO, versao_engine="0.1.0")

    linhas = {linha["variavel"]: linha for linha in _ler(arquivos["g_gap_proveniencia"])}
    renuncia = linhas["Renúncia fiscal (ICMS)"]
    assert renuncia["valor"] == ""
    assert renuncia["fonte_vencedora"] == ""
    assert (linhas["VAB"]["fonte_vencedora"], linhas["VAB"]["ordem_cascata"]) == (
        "ibge_sidra", "2",
    )
