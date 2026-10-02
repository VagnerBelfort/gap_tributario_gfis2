"""Exportação do cálculo anual para a camada ouro do Impala (gfis2_ouro.g_gap_*).

Gera um CSV por tabela, com as colunas na ordem do DDL de produção. O painel da
SEFAZ lê essas tabelas: colunas, tipos e valores categóricos (fonte_*, variavel,
componente) são os da carga de 31/08/2026 e não mudam. Quem grava no Impala é
`jobs/carga_gold_gap.py`, que lê estes CSVs e não faz conta nenhuma.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Dict, List, Optional

from gap_tributario.models import DecomposicaoGap, Proveniencia, ResultadoGap

COLUNAS_RESULTADO = [
    "ano", "tipo_periodo", "nro_trimestre", "vab", "exportacoes", "importacoes",
    "base_calculo", "aliquota", "ptax_media", "icms_potencial", "icms_arrecadado",
    "vrr", "gap_absoluto", "gap_percentual", "flg_parcial", "flg_estimado",
    "fonte_vab", "fonte_icms", "fonte_imp", "fonte_exp", "dt_calculo",
    "versao_engine", "id_execucao",
]

COLUNAS_DECOMPOSICAO = [
    "ano", "componente", "valor", "pct_do_gap", "vintage_ldo",
    "flg_compliance_negativo", "dt_calculo", "id_execucao",
]

COLUNAS_PROVENIENCIA = [
    "ano", "tipo_periodo", "nro_trimestre", "variavel", "valor", "origem", "fonte",
    "dt_extracao", "observacoes", "fonte_vencedora", "ordem_cascata", "dt_calculo",
    "id_execucao",
]

# Posição de cada fonte na sua cascata (1 = primeira opção); override manual
# fica fora da cascata.
_ORDEM_CASCATA = {
    "imesc": 1, "ibge_sidra": 2,
    "gfis2": 1, "sigdef": 2,
    "siscomex": 1, "mdic_bruto": 2,
    "mdic": 1, "bcb_olinda": 1, "config": 1, "amf": 1,
    "manual": 0,
}

# Modalidade da AMF Tabela 7 → componente gravado na ouro.
_COMPONENTE_POR_MODALIDADE = {
    "Crédito Presumido": "MOD_CREDITO_PRESUMIDO",
    "Isenção": "MOD_ISENCAO",
    "Redução de Base de Cálculo": "MOD_REDUCAO_BASE",
}


@dataclass(frozen=True)
class CalculoAnual:
    """Tudo o que o CLI apurou para um ano, na forma que a ouro precisa.

    `fontes` traz a chave da fonte que venceu cada cascata ("imesc",
    "ibge_sidra", "gfis2", "sigdef", "siscomex", "mdic_bruto", "mdic",
    "bcb_olinda", "manual"). `importacoes_por_fonte` separa DI e DUIMP, em
    R$ milhões.
    """

    resultado: ResultadoGap
    decomposicao: Optional[DecomposicaoGap]
    proveniencias: List[Proveniencia]
    fontes: Dict[str, str]
    importacoes_por_fonte: Dict[str, Decimal] = field(default_factory=dict)
    legislacao_aliquota: str = ""


def _q(valor: Decimal, casas: int) -> str:
    return str(valor.quantize(Decimal(1).scaleb(-casas), rounding=ROUND_HALF_UP))


def _linha_resultado(c: CalculoAnual, dt_calculo: str, versao: str, id_execucao: str) -> list:
    r = c.resultado
    return [
        r.periodo.ano, "A", 0,
        _q(r.vab, 2), _q(r.exportacoes_brl, 2), _q(r.importacoes_brl, 2),
        # A base sai do potencial do motor, sem refazer a fórmula aqui.
        _q(r.icms_potencial / r.aliquota_padrao, 2),
        _q(r.aliquota_padrao, 4), _q(r.ptax_media, 4),
        _q(r.icms_potencial, 2), _q(r.icms_arrecadado, 2),
        _q(r.vrr, 6), _q(r.gap_absoluto, 2), _q(r.gap_percentual, 4),
        "false", "false",
        c.fontes["vab"], c.fontes["icms"], c.fontes["imp"], c.fontes["exp"],
        dt_calculo, versao, id_execucao,
    ]


def _linhas_decomposicao(c: CalculoAnual, dt_calculo: str, id_execucao: str) -> list:
    d = c.decomposicao
    if d is None:
        # Ano sem AMF não tem linha: o painel trata a ausência, não o zero.
        return []

    def pct(valor: Decimal) -> Decimal:
        return valor / d.gap_total * Decimal("100") if d.gap_total else Decimal("0")

    componentes = [
        ("GAP_TOTAL", d.gap_total, Decimal("100")),
        ("POLICY", d.policy_gap, d.policy_pct),
        ("COMPLIANCE", d.compliance_gap, d.compliance_pct),
    ] + [
        (_COMPONENTE_POR_MODALIDADE[modalidade], valor, pct(valor))
        for modalidade, valor in d.renuncia.por_modalidade.items()
    ]
    negativo = "true" if d.compliance_negativo else "false"
    return [
        [c.resultado.periodo.ano, nome, _q(valor, 2), _q(percentual, 4),
         d.renuncia.vintage, negativo, dt_calculo, id_execucao]
        for nome, valor, percentual in componentes
    ]


def _milhoes(valor: Decimal) -> str:
    """R$ milhões no formato brasileiro: 27.208,44."""
    return f"{valor:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _linhas_proveniencia(c: CalculoAnual, dt_calculo: str, id_execucao: str) -> list:
    r = c.resultado
    da_cli = {p.variavel: p for p in c.proveniencias}
    data = da_cli["VAB"].data_extracao

    def da_cascata(variavel_ouro, variavel_cli, valor, chave, prefixo=""):
        p = da_cli[variavel_cli]
        obs = " ".join(t for t in (prefixo, p.observacoes) if t)
        return (variavel_ouro, valor, p.origem, p.fonte, p.data_extracao, obs, c.fontes[chave])

    composicao = " + ".join(
        f"{fonte} R$ {_milhoes(valor)} mi" for fonte, valor in c.importacoes_por_fonte.items()
    )
    variaveis = [
        da_cascata("VAB", "VAB", r.vab, "vab"),
        da_cascata("ICMS Arrecadado", "ICMS Arrecadado", r.icms_arrecadado, "icms"),
        da_cascata("Exportações", "Exportações", r.exportacoes_brl, "exp"),
        da_cascata("Importações", "Importações", r.importacoes_brl, "imp",
                   prefixo=f"{composicao}." if composicao else ""),
        da_cascata("PTAX média", "Câmbio (PTAX)", r.ptax_media, "ptax"),
        ("Alíquota modal", r.aliquota_padrao, "config/aliquotas.yaml",
         c.legislacao_aliquota, data, "", "config"),
    ]
    if c.decomposicao is not None:
        p = da_cli["Renúncia Fiscal (ICMS)"]
        variaveis.append(("Renúncia fiscal (ICMS)", c.decomposicao.renuncia.total, p.origem,
                          p.fonte, p.data_extracao, p.observacoes, "amf"))
    else:
        variaveis.append(("Renúncia fiscal (ICMS)", None, "AMF Tabela 7 (LDO/MA)",
                          f"Sem cobertura para {r.periodo.ano}", data,
                          "Cobertura AMF inicia em 2022", None))

    return [
        [r.periodo.ano, "A", 0, variavel, "" if valor is None else _q(valor, 4),
         origem, fonte, dt_extracao, obs, vencedora or "",
         _ORDEM_CASCATA[vencedora] if vencedora else 1, dt_calculo, id_execucao]
        for variavel, valor, origem, fonte, dt_extracao, obs, vencedora in variaveis
    ]


def _gravar(caminho: Path, colunas: list, linhas: list) -> Path:
    with caminho.open("w", encoding="utf-8", newline="") as f:
        escritor = csv.writer(f)
        escritor.writerow(colunas)
        escritor.writerows(linhas)
    return caminho


def exportar_ouro(
    calculos: List[CalculoAnual],
    destino: Path,
    dt_calculo: datetime,
    versao_engine: str,
) -> Dict[str, Path]:
    """Grava os CSVs da ouro em `destino`, um por tabela.

    Returns:
        Nome da tabela → caminho do CSV gravado.
    """
    destino.mkdir(parents=True, exist_ok=True)
    ts = dt_calculo.strftime("%Y-%m-%d %H:%M:%S")
    id_execucao = f"CARGA_CLI_{dt_calculo.date().isoformat()}"

    return {
        "g_gap_resultado": _gravar(
            destino / "g_gap_resultado.csv",
            COLUNAS_RESULTADO,
            [_linha_resultado(c, ts, versao_engine, id_execucao) for c in calculos],
        ),
        "g_gap_decomposicao": _gravar(
            destino / "g_gap_decomposicao.csv",
            COLUNAS_DECOMPOSICAO,
            [linha for c in calculos for linha in _linhas_decomposicao(c, ts, id_execucao)],
        ),
        "g_gap_proveniencia": _gravar(
            destino / "g_gap_proveniencia.csv",
            COLUNAS_PROVENIENCIA,
            [linha for c in calculos for linha in _linhas_proveniencia(c, ts, id_execucao)],
        ),
    }
