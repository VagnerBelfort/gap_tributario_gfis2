#!/usr/bin/env python3
"""Cria as tabelas da camada ouro do Gap Tributário e carrega a série apurada.

Nasceu como seed de protótipo, para destravar o front-end antes do pipeline
real existir. Desde 31/08/2026 carrega **dados apurados**: a série anual
2020-2025 produzida pelo CLI `gap_tributario`, conferida contra fontes
independentes.

    ┌──────────────────────────────────────────────────────────────────┐
    │  OS VALORES AQUI SÃO APURADOS — não são mais protótipo           │
    │                                                                  │
    │  Origem: saída de `python -m gap_tributario --periodo AAAA`.     │
    │    · ICMS         → GFIS2 g_arrecadacao, TODAS as parcelas de    │
    │      ICMS. Converge com o SIGDEF/CONFAZ dentro de ~1%.           │
    │    · Importações  → Siscomex, domicílio fiscal (CIF). Desvio     │
    │      contra o MDIC dentro da faixa medida de +2,4% a +12,8%.     │
    │    · Série começa em 2020: o GFIS2 de 2019 é parcial (entra em   │
    │      regime só a partir de agosto).                              │
    │                                                                  │
    │  Ainda é uma carga MANUAL: o cálculo roda fora do cluster e os   │
    │  valores são transcritos abaixo. O pipeline que calcula dentro   │
    │  do Spark (jobs/gold_calcula.py) continua pendente — enquanto    │
    │  não existir, reexecutar este job após qualquer mudança de       │
    │  fonte ou de fórmula.                                            │
    │                                                                  │
    │  Toda linha carrega id_execucao = 'CARGA_MANUAL_2026-08-31'.     │
    └──────────────────────────────────────────────────────────────────┘

O DDL é idêntico ao de produção: só muda o database.

Uso (a partir de /gfis2/pipeline, como os demais jobs):
    ./gap_tributario/run_seed_gold_gap.sh --database gfis2_ouro

    # só cria as tabelas, sem semear (ex.: preparar produção vazia)
    ./gap_tributario/run_seed_gold_gap.sh --database gfis2_ouro --apenas-ddl

As tabelas são Iceberg — o submit precisa do jar e do catálogo, conforme
3-gold/processamento_pyspark/run_g_arrecadacao.sh:

    spark3-submit --master yarn --deploy-mode client \\
      --jars /gfis2/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \\
      --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \\
      --conf spark.sql.catalog.spark_catalog.type=hive \\
      gap_tributario/seed_gold_mock.py --database gfis2_ouro

Não há etapa de HDFS: os dados estão embutidos neste script, não em arquivos.
O `hdfs dfs -put` do 2-silver serve para mover parquets de dados — aqui não há
parquet de origem. Mesmo formato do job de gold, que também é só SQL.

Depois de rodar, para o Impala enxergar as tabelas novas:
    impala-shell -q "INVALIDATE METADATA gfis2_ouro.g_gap_resultado;"
    impala-shell -q "INVALIDATE METADATA gfis2_ouro.g_gap_decomposicao;"
    impala-shell -q "INVALIDATE METADATA gfis2_ouro.g_gap_proveniencia;"

Referência de design: docs/pipeline-impala-medallion.md
"""


# NOTA: sem `from __future__ import annotations` de propósito. O driver deste
# cluster roda Python 3.6.8 (após o `conda deactivate` do run script), e esse
# import exige 3.7+. Os jobs existentes em 3-gold/ também não o usam — este
# arquivo segue a convenção da casa, não a do pyproject.toml (que pede >=3.8).
import argparse
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Marca de água: toda linha semeada por este script carrega esta chave.
# Deixou de ser protótipo — os valores abaixo são a saída real do CLI.
ID_EXECUCAO_MOCK = "CARGA_MANUAL_2026-08-31"
VERSAO_ENGINE_MOCK = "gap_tributario-cli-2026-08-31"


# ─────────────────────────────────────────────────────────────────────────────
# DDL — idêntico ao de produção; {db} é o único parâmetro.
# Canônico: se o DBA precisar do SQL, é este texto.
# ─────────────────────────────────────────────────────────────────────────────

DDL = [
    """
    CREATE DATABASE IF NOT EXISTS {db}
    COMMENT 'Gap Tributário ICMS-MA — metodologia VAT/VRR (OCDE)'
    """,
    """
    CREATE TABLE IF NOT EXISTS {db}.g_gap_resultado (
      ano              INT           COMMENT 'Ano de referência',
      tipo_periodo     STRING        COMMENT 'A = anual | T = trimestral',
      nro_trimestre    INT           COMMENT '0 quando anual, 1-4 quando trimestral',
      vab              DECIMAL(18,2) COMMENT 'Valor Adicionado Bruto — R$ milhões',
      exportacoes      DECIMAL(18,2) COMMENT 'Exportações FOB × PTAX — R$ milhões',
      importacoes      DECIMAL(18,2) COMMENT 'Importações FOB × PTAX — R$ milhões',
      base_calculo     DECIMAL(18,2) COMMENT 'VAB - Exportações + Importações',
      aliquota         DECIMAL(6,4)  COMMENT 'Alíquota modal: 0.18 até 2022, 0.20 de 2023 (Lei 11.867/2022)',
      ptax_media       DECIMAL(10,4) COMMENT 'USD/BRL médio do período (BCB)',
      icms_potencial   DECIMAL(18,2) COMMENT 'base_calculo × aliquota — R$ milhões',
      icms_arrecadado  DECIMAL(18,2) COMMENT 'R$ milhões',
      vrr              DECIMAL(10,6) COMMENT 'icms_arrecadado / icms_potencial — 0 a 1',
      gap_absoluto     DECIMAL(18,2) COMMENT 'icms_potencial - icms_arrecadado',
      gap_percentual   DECIMAL(8,4)  COMMENT '(gap_absoluto / icms_potencial) × 100',
      flg_parcial      BOOLEAN       COMMENT 'período incompleto (ex.: 2026 só T1-T2)',
      flg_estimado     BOOLEAN       COMMENT 'VAB estimado — IBGE tem lag de ~2 anos',
      fonte_vab        STRING        COMMENT 'imesc | ibge_sidra | manual',
      fonte_icms       STRING        COMMENT 'gfis2 | sigdef',
      fonte_imp        STRING        COMMENT 'siscomex | mdic_ncm_filtrado | mdic_bruto',
      fonte_exp        STRING        COMMENT 'mdic',
      dt_calculo       TIMESTAMP     COMMENT 'quando o cálculo rodou',
      versao_engine    STRING        COMMENT 'gap_tributario.__version__',
      id_execucao      STRING        COMMENT 'Airflow run_id — SEED_MOCK_PROTOTIPO quando fictício'
    )
    USING iceberg
    COMMENT 'Resultado VRR por período. Grão: (ano, tipo_periodo, nro_trimestre).'
    TBLPROPERTIES ('transactional'='false')
    """,
    """
    CREATE TABLE IF NOT EXISTS {db}.g_gap_decomposicao (
      ano                     INT,
      componente              STRING        COMMENT 'GAP_TOTAL | POLICY | COMPLIANCE | MOD_CREDITO_PRESUMIDO | MOD_ISENCAO | MOD_REDUCAO_BASE',
      valor                   DECIMAL(18,2) COMMENT 'R$ milhões',
      pct_do_gap              DECIMAL(8,4)  COMMENT '% do gap total',
      vintage_ldo             STRING        COMMENT 'LDO-2022 | LDO-2025 | LDO-2026 — a renúncia é estimativa prospectiva',
      flg_compliance_negativo BOOLEAN       COMMENT 'TRUE quando renúncia > gap total',
      dt_calculo              TIMESTAMP,
      id_execucao             STRING
    )
    USING iceberg
    COMMENT 'Decomposição RA-GAP (FMI): policy × compliance. Anual — a AMF Tabela 7 é anual. Anos sem cobertura NÃO têm linha.'
    TBLPROPERTIES ('transactional'='false')
    """,
    """
    CREATE TABLE IF NOT EXISTS {db}.g_gap_proveniencia (
      ano             INT,
      tipo_periodo    STRING,
      nro_trimestre   INT,
      variavel        STRING        COMMENT 'VAB | ICMS Arrecadado | Exportações | Importações | PTAX média | Alíquota modal | Renúncia fiscal (ICMS)',
      valor           DECIMAL(18,4),
      origem          STRING        COMMENT 'sistema/instituição',
      fonte           STRING        COMMENT 'documento/tabela específica',
      dt_extracao     STRING        COMMENT 'ISO YYYY-MM-DD',
      observacoes     STRING        COMMENT 'caveats de cobertura/metodologia',
      fonte_vencedora STRING        COMMENT 'chave da fonte eleita pela cascata',
      ordem_cascata   INT           COMMENT 'posição da vencedora em fontes.yaml (1 = primeira opção)',
      dt_calculo      TIMESTAMP,
      id_execucao     STRING
    )
    USING iceberg
    COMMENT 'Rastreabilidade de cada variável até a fonte. Espelha o dataclass Proveniencia (models.py).'
    TBLPROPERTIES ('transactional'='false')
    """,
]


# ─────────────────────────────────────────────────────────────────────────────
# Schemas Spark — espelham o DDL. DecimalType casa com o Decimal do MotorVRR.
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_RESULTADO = StructType([
    StructField("ano", IntegerType(), False),
    StructField("tipo_periodo", StringType(), False),
    StructField("nro_trimestre", IntegerType(), False),
    StructField("vab", DecimalType(18, 2), True),
    StructField("exportacoes", DecimalType(18, 2), True),
    StructField("importacoes", DecimalType(18, 2), True),
    StructField("base_calculo", DecimalType(18, 2), True),
    StructField("aliquota", DecimalType(6, 4), True),
    StructField("ptax_media", DecimalType(10, 4), True),
    StructField("icms_potencial", DecimalType(18, 2), True),
    StructField("icms_arrecadado", DecimalType(18, 2), True),
    StructField("vrr", DecimalType(10, 6), True),
    StructField("gap_absoluto", DecimalType(18, 2), True),
    StructField("gap_percentual", DecimalType(8, 4), True),
    StructField("flg_parcial", BooleanType(), True),
    StructField("flg_estimado", BooleanType(), True),
    StructField("fonte_vab", StringType(), True),
    StructField("fonte_icms", StringType(), True),
    StructField("fonte_imp", StringType(), True),
    StructField("fonte_exp", StringType(), True),
    StructField("dt_calculo", TimestampType(), True),
    StructField("versao_engine", StringType(), True),
    StructField("id_execucao", StringType(), True),
])

SCHEMA_DECOMPOSICAO = StructType([
    StructField("ano", IntegerType(), False),
    StructField("componente", StringType(), False),
    StructField("valor", DecimalType(18, 2), True),
    StructField("pct_do_gap", DecimalType(8, 4), True),
    StructField("vintage_ldo", StringType(), True),
    StructField("flg_compliance_negativo", BooleanType(), True),
    StructField("dt_calculo", TimestampType(), True),
    StructField("id_execucao", StringType(), True),
])

SCHEMA_PROVENIENCIA = StructType([
    StructField("ano", IntegerType(), False),
    StructField("tipo_periodo", StringType(), False),
    StructField("nro_trimestre", IntegerType(), False),
    StructField("variavel", StringType(), False),
    StructField("valor", DecimalType(18, 4), True),
    StructField("origem", StringType(), True),
    StructField("fonte", StringType(), True),
    StructField("dt_extracao", StringType(), True),
    StructField("observacoes", StringType(), True),
    StructField("fonte_vencedora", StringType(), True),
    StructField("ordem_cascata", IntegerType(), True),
    StructField("dt_calculo", TimestampType(), True),
    StructField("id_execucao", StringType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
# Dados do protótipo — transcritos de "Gap Tributario GFIS2 v2.dc.html", db().
# Valores em R$ milhões. FICTÍCIOS. Ver aviso no topo do arquivo.
# ─────────────────────────────────────────────────────────────────────────────

D = Decimal

RAW: List[Dict] = [
    # Saída real de `python -m gap_tributario --periodo AAAA`, 31/08/2026.
    # ICMS: GFIS2 g_arrecadacao, soma de todas as parcelas de ICMS.
    # Importações: Siscomex, domicílio fiscal do importador (CIF).
    # A série começa em 2020: em 2019 o GFIS2 só entra em regime a partir de
    # agosto, o que produzia um VRR artificialmente baixo (0,213).
    {"ano": 2020, "aliq": D("0.18"), "ptax": D("5.1578"), "vab": D("95497.34"),
     "icms": D("8195.78"), "exp": D("17387.76"), "imp": D("11504.37"),
     "ren": None, "fonte_vab": "ibge_sidra"},
    {"ano": 2021, "aliq": D("0.18"), "ptax": D("5.3956"), "vab": D("110230"),
     "icms": D("9744.74"), "exp": D("23600.89"), "imp": D("24102.07"),
     "ren": None, "fonte_vab": "imesc"},
    {"ano": 2022, "aliq": D("0.18"), "ptax": D("5.1655"), "vab": D("124859"),
     "icms": D("11394.47"), "exp": D("29639.40"), "imp": D("39703.79"),
     "ren": D("2182.131338"), "vintage": "LDO-2022", "fonte_vab": "imesc"},
    {"ano": 2023, "aliq": D("0.20"), "ptax": D("4.9953"), "vab": D("135434"),
     "icms": D("10824.28"), "exp": D("27377.56"), "imp": D("25650.15"),
     "ren": D("2253.050607"), "vintage": "LDO-2022", "fonte_vab": "imesc"},
    {"ano": 2024, "aliq": D("0.20"), "ptax": D("5.3920"), "vab": D("144220"),
     "icms": D("13945.58"), "exp": D("30189.93"), "imp": D("23344.39"),
     "ren": D("2326.274752"), "vintage": "LDO-2022", "fonte_vab": "imesc"},
    {"ano": 2025, "aliq": D("0.20"), "ptax": D("5.5859"), "vab": D("156260"),
     "icms": D("15714.22"), "exp": D("28055.73"), "imp": D("27208.42"),
     "ren": D("2371.561069"), "vintage": "LDO-2025", "fonte_vab": "imesc"},
]

# Sem linhas trimestrais nesta carga: o rateio sazonal do protótipo era
# fabricado, e publicar trimestre estimado como se fosse apuração repetiria o
# erro que acabamos de corrigir. O CLI já calcula trimestre de verdade
# (--periodo AAAA-TN); publicar isso na camada ouro é item de próximos passos.
NQ_PADRAO = 0

# Pesos de rateio trimestral do protótipo (somam 1,0 cada).
SHARE_ICMS = [D("0.235"), D("0.245"), D("0.270"), D("0.250")]
SHARE_EXP = [D("0.240"), D("0.260"), D("0.270"), D("0.230")]
SHARE_IMP = [D("0.250"), D("0.240"), D("0.270"), D("0.240")]

# Modalidades de renúncia (AMF Tabela 7) e seus pesos no protótipo.
MODALIDADES = [
    ("MOD_CREDITO_PRESUMIDO", D("0.581")),
    ("MOD_ISENCAO", D("0.238")),
    ("MOD_REDUCAO_BASE", D("0.181")),
]


def q2(v: Decimal) -> Decimal:
    return v.quantize(D("0.01"), rounding=ROUND_HALF_UP)


def q4(v: Decimal) -> Decimal:
    return v.quantize(D("0.0001"), rounding=ROUND_HALF_UP)


def q6(v: Decimal) -> Decimal:
    return v.quantize(D("0.000001"), rounding=ROUND_HALF_UP)


def calcular(vab: Decimal, exp: Decimal, imp: Decimal, icms: Decimal,
             aliq: Decimal) -> Dict[str, Decimal]:
    """Fórmula VRR (OCDE) — mesma de engine/vrr.py, replicada sem importar o pacote.

    Este script roda antes do gap_engine.zip existir no cluster, então não pode
    importar o MotorVRR. A duplicação é deliberada e TEMPORÁRIA: quando o
    jobs/gold_calcula.py entrar, ele importa o motor de verdade e este seed morre.
    A fórmula é a mesma: Base = VAB - Exp + Imp; Potencial = Base × Alíquota.
    """
    base = vab - exp + imp
    potencial = base * aliq
    return {
        "base": base,
        "potencial": potencial,
        "vrr": icms / potencial,
        "gap": potencial - icms,
        "gap_pct": (potencial - icms) / potencial * D("100"),
    }


def linha_resultado(ano: int, tipo: str, tri: int, vab, exp, imp, icms, aliq,
                    ptax, parcial: bool, estimado: bool, ts, fonte_vab="imesc"):
    c = calcular(vab, exp, imp, icms, aliq)
    return (
        ano, tipo, tri,
        q2(vab), q2(exp), q2(imp), q2(c["base"]), q4(aliq), q4(ptax),
        q2(c["potencial"]), q2(icms), q6(c["vrr"]), q2(c["gap"]), q4(c["gap_pct"]),
        parcial, estimado,
        # Importação vem do Siscomex (domicílio fiscal), não do MDIC.
        fonte_vab, "gfis2", "siscomex", "mdic",
        ts, VERSAO_ENGINE_MOCK, ID_EXECUCAO_MOCK,
    )


def construir_resultado(ts) -> List[tuple]:
    linhas = []
    for r in RAW:
        nq = r.get("nq", NQ_PADRAO)
        parcial = r.get("parcial", False)
        estimado = r.get("estimado", False)

        linhas.append(linha_resultado(
            r["ano"], "A", 0, r["vab"], r["exp"], r["imp"], r["icms"],
            r["aliq"], r["ptax"], parcial, estimado, ts,
            r.get("fonte_vab", "imesc")))

        for i in range(nq):
            # Ano parcial rateia igualmente; ano completo usa os pesos sazonais.
            qv = r["vab"] / nq
            qe = r["exp"] / nq if parcial else r["exp"] * SHARE_EXP[i]
            qm = r["imp"] / nq if parcial else r["imp"] * SHARE_IMP[i]
            qi = r["qicms"][i] if "qicms" in r else r["icms"] * SHARE_ICMS[i]
            linhas.append(linha_resultado(
                r["ano"], "T", i + 1, qv, qe, qm, qi,
                r["aliq"], r["ptax"], parcial, estimado, ts,
                r.get("fonte_vab", "imesc")))
    return linhas


def construir_decomposicao(ts) -> List[tuple]:
    """Só anos anuais completos COM renúncia. 2019-2021 e 2026 ficam de fora."""
    linhas = []
    for r in RAW:
        if r.get("ren") is None or r.get("parcial", False):
            continue  # sem cobertura AMF (2019-2021) ou ano parcial (2026)

        c = calcular(r["vab"], r["exp"], r["imp"], r["icms"], r["aliq"])
        gap_total = c["gap"]
        policy = r["ren"]
        compliance = gap_total - policy
        policy_pct = policy / gap_total * D("100")
        vin = r["vintage"]
        neg = compliance < 0

        linhas.append((r["ano"], "GAP_TOTAL", q2(gap_total), q4(D("100")), vin, neg, ts, ID_EXECUCAO_MOCK))
        linhas.append((r["ano"], "POLICY", q2(policy), q4(policy_pct), vin, neg, ts, ID_EXECUCAO_MOCK))
        linhas.append((r["ano"], "COMPLIANCE", q2(compliance), q4(D("100") - policy_pct), vin, neg, ts, ID_EXECUCAO_MOCK))
        for nome, peso in MODALIDADES:
            linhas.append((r["ano"], nome, q2(policy * peso), q4(peso * policy_pct), vin, neg, ts, ID_EXECUCAO_MOCK))
    return linhas


def construir_proveniencia(ts) -> List[tuple]:
    """Uma linha por variável por período — inclusive trimestres."""
    linhas = []
    for r in RAW:
        ano, estimado = r["ano"], r.get("estimado", False)
        nq = r.get("nq", NQ_PADRAO)
        legislacao = ("Lei Estadual vigente até 2022" if r["aliq"] == D("0.18")
                      else "Lei 11.867/2022 — alíquota modal 20%")

        periodos = [("A", 0, r["vab"], r["exp"], r["imp"], r["icms"])]
        for i in range(nq):
            periodos.append((
                "T", i + 1,
                r["vab"] / nq,
                r["exp"] / nq if r.get("parcial") else r["exp"] * SHARE_EXP[i],
                r["imp"] / nq if r.get("parcial") else r["imp"] * SHARE_IMP[i],
                r["qicms"][i] if "qicms" in r else r["icms"] * SHARE_ICMS[i],
            ))

        for tipo, tri, vab, exp, imp, icms in periodos:
            vars_ = [
                ("VAB", vab,
                 "IMESC / IBGE SIDRA 5938",
                 "Relatório PIB Trimestral (IMESC); fallback IBGE Contas Regionais",
                 "Estimado — IBGE publica com ~2 anos de lag" if estimado
                 else "Lag de ~2 anos na fonte IBGE",
                 r.get("fonte_vab", "imesc"),
                 2 if r.get("fonte_vab") == "ibge_sidra" else 1),
                ("ICMS Arrecadado", icms,
                 "GFIS2 (gfis2_ouro.g_arrecadacao)",
                 "Soma de todas as parcelas de ICMS (normal, importação, ST saída, "
                 "ST entrada, dívida ativa, TVI, FCP, FDI, IDH, fruição)",
                 "Converge com o SIGDEF/CONFAZ dentro de ~1% em 2020-2023",
                 "gfis2", 1),
                ("Exportações", exp,
                 "MDIC ComexStat", f"EXP_{ano}.csv — FOB USD × PTAX",
                 "", "mdic", 1),
                ("Importações", imp,
                 "Siscomex (APL_SISCOMEX / SEFAZ-MA)",
                 "TDS_UF_IMPORTADOR — domicílio fiscal, valor aduaneiro (CIF)",
                 "Validação cruzada contra MDIC ComexStat: desvio dentro da faixa "
                 "observada de +2,4% a +12,8% (assinatura CIF sobre FOB)",
                 "siscomex", 1),
                ("PTAX média", r["ptax"],
                 "BCB API Olinda", "Média de cotacaoVenda dos dias úteis do período",
                 "", "bcb_olinda", 1),
                ("Alíquota modal", r["aliq"],
                 "config/aliquotas.yaml", legislacao, "", "config", 1),
                ("Renúncia fiscal (ICMS)", r.get("ren"),
                 "AMF Tabela 7 (LDO/MA)",
                 r.get("vintage") or f"Sem cobertura para {ano}",
                 "Estimativa prospectiva da LDO" if r.get("ren") is not None
                 else "Cobertura AMF inicia em 2022",
                 "amf" if r.get("ren") is not None else None, 1),
            ]
            for variavel, valor, origem, fonte, obs, vencedora, ordem in vars_:
                linhas.append((
                    ano, tipo, tri, variavel,
                    q4(valor) if valor is not None else None,
                    origem, fonte, "2026-07-15", obs, vencedora, ordem,
                    ts, ID_EXECUCAO_MOCK,
                ))
    return linhas


def main(database: str, apenas_ddl: bool, pular_ddl: bool) -> None:
    spark = (SparkSession.builder
             .appName(f"gap_seed_gold_{database}")
             .enableHiveSupport()
             .getOrCreate())

    if not pular_ddl:
        print(f"[DDL] criando tabelas em {database}...")
        for stmt in DDL:
            spark.sql(stmt.format(db=database))
        print(f"[DDL] ok — 3 tabelas em {database}")

    if apenas_ddl:
        print("[SEED] pulado (--apenas-ddl). Tabelas criadas vazias.")
        spark.stop()
        return

    ts = spark.sql("SELECT current_timestamp() AS t").collect()[0]["t"]

    conjuntos = [
        ("g_gap_resultado", construir_resultado(ts), SCHEMA_RESULTADO),
        ("g_gap_decomposicao", construir_decomposicao(ts), SCHEMA_DECOMPOSICAO),
        ("g_gap_proveniencia", construir_proveniencia(ts), SCHEMA_PROVENIENCIA),
    ]

    for tabela, linhas, schema in conjuntos:
        df = spark.createDataFrame(linhas, schema=schema)
        view = f"tmp_seed_{tabela}"
        df.createOrReplaceTempView(view)
        # temp view + INSERT OVERWRITE: mesmo idioma dos jobs de gold existentes
        # (3-gold/processamento_pyspark/pyspark_processa_g_arrecadacao_dev.py).
        # Tabela Iceberg não particionada → o overwrite substitui o conteúdo
        # inteiro, o que mantém o seed idempotente.
        spark.sql(f"INSERT OVERWRITE TABLE {database}.{tabela} SELECT * FROM {view}")
        print(f"[SEED] {database}.{tabela}: {len(linhas)} linhas")

    print()
    print("=" * 72)
    print(f"  Dados de PROTÓTIPO semeados em {database}")
    print(f"  Marca de água: id_execucao = '{ID_EXECUCAO_MOCK}'")
    print()
    print("  Agora rode, para o Impala enxergar as tabelas novas:")
    print(f"    impala-shell -i $IMPALA_HOST -q 'INVALIDATE METADATA {database};'")
    print("=" * 72)

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--database", default="gfis2_dev",
                   help="database alvo (default: gfis2_dev — NUNCA gfis2_ouro com seed)")
    p.add_argument("--apenas-ddl", action="store_true",
                   help="só cria as tabelas, sem semear dados")
    p.add_argument("--pular-ddl", action="store_true",
                   help="assume tabelas já criadas (ex.: DDL rodado pelo DBA)")
    a = p.parse_args()
    main(a.database, a.apenas_ddl, a.pular_ddl)
