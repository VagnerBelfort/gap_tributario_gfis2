"""Regras do job de carga da ouro do Gap Tributário (jobs/carga_gold_gap.py).

Python puro e compatível com o 3.6.8 do driver do cluster: sem pyspark, sem
dataclasses, sem `from __future__ import annotations`. Fica separado do job
para ser testado no pytest local; o job só lê, chama estas funções e grava.

O painel da SEFAZ lê as tabelas gfis2_ouro.g_gap_*. A carga aborta se a
estrutura ou um valor categórico divergir do de produção, e só avisa quando o
ICMS do CSV se afasta da g_arrecadacao do momento (arrecadação tardia e dívida
ativa movem os anos recentes de forma legítima).
"""

from decimal import Decimal

# Schema de produção, no formato de `DataFrame.dtypes` do Spark. É o DDL da
# carga de 31/08/2026 e não muda: o painel depende dele.
SCHEMA_OURO = {
    "g_gap_resultado": [
        ("ano", "int"),
        ("tipo_periodo", "string"),
        ("nro_trimestre", "int"),
        ("vab", "decimal(18,2)"),
        ("exportacoes", "decimal(18,2)"),
        ("importacoes", "decimal(18,2)"),
        ("base_calculo", "decimal(18,2)"),
        ("aliquota", "decimal(6,4)"),
        ("ptax_media", "decimal(10,4)"),
        ("icms_potencial", "decimal(18,2)"),
        ("icms_arrecadado", "decimal(18,2)"),
        ("vrr", "decimal(10,6)"),
        ("gap_absoluto", "decimal(18,2)"),
        ("gap_percentual", "decimal(8,4)"),
        ("flg_parcial", "boolean"),
        ("flg_estimado", "boolean"),
        ("fonte_vab", "string"),
        ("fonte_icms", "string"),
        ("fonte_imp", "string"),
        ("fonte_exp", "string"),
        ("dt_calculo", "timestamp"),
        ("versao_engine", "string"),
        ("id_execucao", "string"),
    ],
    "g_gap_decomposicao": [
        ("ano", "int"),
        ("componente", "string"),
        ("valor", "decimal(18,2)"),
        ("pct_do_gap", "decimal(8,4)"),
        ("vintage_ldo", "string"),
        ("flg_compliance_negativo", "boolean"),
        ("dt_calculo", "timestamp"),
        ("id_execucao", "string"),
    ],
    "g_gap_proveniencia": [
        ("ano", "int"),
        ("tipo_periodo", "string"),
        ("nro_trimestre", "int"),
        ("variavel", "string"),
        ("valor", "decimal(18,4)"),
        ("origem", "string"),
        ("fonte", "string"),
        ("dt_extracao", "string"),
        ("observacoes", "string"),
        ("fonte_vencedora", "string"),
        ("ordem_cascata", "int"),
        ("dt_calculo", "timestamp"),
        ("id_execucao", "string"),
    ],
}

# Colunas cujo conjunto de valores o painel conhece. Valor que produção nunca
# viu aborta a carga.
CATEGORICAS = {
    "g_gap_resultado": ["tipo_periodo", "fonte_vab", "fonte_icms", "fonte_imp", "fonte_exp"],
    "g_gap_decomposicao": ["componente"],
    "g_gap_proveniencia": ["tipo_periodo", "variavel", "fonte_vencedora"],
}

# Mesma lista de `extractors/arrecadacao.py::_COLUNAS_ICMS` (um teste garante).
COLUNAS_ICMS = [
    "val_icms_normal",
    "val_icms_imp",
    "val_icms_st_sda",
    "val_icms_st_ent",
    "val_icms_da",
    "val_icms_tvi",
    "val_fcp",
    "val_fdi",
    "val_idh",
    "val_fruicao_ben_fiscal",
]

TOLERANCIA_ICMS_PCT = Decimal("0.5")
# CSV e prata saem da mesma extração: a tolerância só absorve o arredondamento
# do CSV a centavos de milhão (0,0001% de R$ 27 bi = R$ 0,03 mi).
TOLERANCIA_IMPORTACOES_PCT = Decimal("0.0001")


def diferencas_colunas(esperadas, encontradas):
    """Diferenças entre duas listas ordenadas de colunas (nome ou (nome, tipo)).

    Returns:
        Lista de mensagens; vazia quando as listas são idênticas.
    """
    if list(esperadas) == list(encontradas):
        return []
    problemas = []
    faltando = [c for c in esperadas if c not in encontradas]
    sobrando = [c for c in encontradas if c not in esperadas]
    if faltando:
        problemas.append(f"esperadas e ausentes: {faltando}")
    if sobrando:
        problemas.append(f"presentes e não esperadas: {sobrando}")
    if not problemas:
        problemas.append(
            f"mesmas colunas em outra ordem: esperado {list(esperadas)}, encontrado {list(encontradas)}"
        )
    return problemas


def valores_ineditos(novos, producao):
    """Valores categóricos da carga que a produção nunca teve.

    Args:
        novos: coluna → conjunto de valores distintos da carga.
        producao: coluna → conjunto de valores distintos hoje em produção.
    """
    problemas = []
    for coluna in sorted(novos):
        ineditos = set(novos[coluna]) - set(producao.get(coluna, set()))
        if ineditos:
            problemas.append(
                f"{coluna}: valores que a produção não tem: {sorted(ineditos)}"
            )
    return problemas


def avisos_divergencia(variavel, valores_csv, referencia, tolerancia_pct):
    """Anos em que o valor do CSV se afasta da referência além da tolerância.

    Args:
        variavel: rótulo da mensagem ("ICMS", "Importações").
        valores_csv: ano → valor gravado no CSV (R$ milhões).
        referencia: ano → valor de referência no cluster agora (R$ milhões).
        tolerancia_pct: desvio absoluto máximo, em %.
    """
    avisos = []
    for ano in sorted(valores_csv):
        atual = referencia.get(ano)
        if not atual:
            avisos.append(f"{variavel} {ano}: sem valor de referência para comparar")
            continue
        valor = Decimal(valores_csv[ano])
        desvio = (valor - Decimal(atual)) / Decimal(atual) * 100
        if abs(desvio) > tolerancia_pct:
            avisos.append(
                f"{variavel} {ano}: CSV {valor:.2f} × referência {Decimal(atual):.2f} (desvio {desvio:+.4f}%)"
            )
    return avisos
