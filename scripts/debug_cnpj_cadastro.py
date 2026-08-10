#!/usr/bin/env python3
"""Depura o join Siscomex x b_contribuinte — versão 3.

Aprendido nas versões anteriores:
  - `cnpj_raiz` tem 8 dígitos (ou 11, quando CPF): não casa com o CNPJ completo.
    A coluna certa é `cnpj`, e com ela o join FUNCIONA (673/797 no controle).
  - A coluna `uf` é NULA em todas as linhas que têm CNPJ — ela só é preenchida
    nas linhas de pessoa física. Por isso o snapshot v1 não resolveu nada.

Falta descobrir qual coluna carrega a UF do contribuinte pessoa jurídica.
Candidatas vistas no silver: uf_icms, uf_ipva, municipio, cidade_icms.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/debug_cnpj_cadastro.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_URL = "jdbc:oracle:thin:@10.1.1.132:1521/cent"
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit(
        "Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.\n"
        "  export SISCOMEX_ORACLE_PASSWORD='...'"
    )
DRIVER = "oracle.jdbc.driver.OracleDriver"
TABELA_CADASTRO = "gfis2_bronze.b_contribuinte"

# Candidatas a UF do contribuinte PJ, em ordem de preferência para o domicílio
# fiscal de ICMS.
_CANDIDATAS = ["uf_icms", "uf", "uf_ipva"]

_CTE = """
    WITH ultima AS (
        SELECT TDS_NUM_DECL, MAX(TDS_SEQ_DECL) AS SEQ
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_NUM_DECL
    )
"""


def _sql(filtro_uf):
    return f"""
    {_CTE}
    SELECT DISTINCT d.TDS_CNPJ AS cnpj
      FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
      JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
     WHERE {filtro_uf}
"""


def _chave(coluna):
    return F.lpad(F.regexp_replace(coluna.cast("string"), r"\D", ""), 14, "0")


def ler_oracle(spark, sql):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", sql)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .load()
    )


def main():
    spark = (
        SparkSession.builder.appName("Debug CNPJ Cadastro v3")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bruto = spark.table(TABELA_CADASTRO).filter(F.col("cnpj").isNotNull())
    disponiveis = [c for c in _CANDIDATAS if c in bruto.columns]
    print("\n=== COLUNAS CANDIDATAS PRESENTES ===", disponiveis)

    print("\n=== PREENCHIMENTO DAS CANDIDATAS (só linhas com CNPJ) ===")
    bruto.select(
        F.count("*").alias("linhas_com_cnpj"),
        *[
            F.sum(F.col(c).isNotNull().cast("int")).alias(f"preenchido_{c}")
            for c in disponiveis
        ],
    ).show(truncate=False)

    for c in disponiveis:
        print(f"--- valores mais comuns de {c} (linhas com CNPJ) ---")
        bruto.groupBy(c).count().orderBy(F.desc("count")).show(6, truncate=False)

    # Uma linha por CNPJ, com a primeira UF não-nula entre as candidatas.
    cad = bruto.select(
        _chave(F.col("cnpj")).alias("k"),
        F.coalesce(*[F.trim(F.col(c)) for c in disponiveis]).alias("uf_cad"),
    )
    cad = (
        cad.withColumn("prio", F.when(F.col("uf_cad").isNull(), 1).otherwise(0))
        .orderBy("prio")
        .dropDuplicates(["k"])
    )

    grupos = {
        "CONTROLE (UF=MA no Siscomex)": _sql("d.TDS_UF_IMPORTADOR = 'MA'"),
        "CONTROLE (UF<>MA no Siscomex)": _sql(
            "d.TDS_UF_IMPORTADOR IS NOT NULL AND d.TDS_UF_IMPORTADOR <> 'MA'"
        ),
        "SEM UF": _sql("d.TDS_UF_IMPORTADOR IS NULL"),
    }

    for nome, sql in grupos.items():
        alvo = ler_oracle(spark, sql).select(_chave(F.col("cnpj")).alias("k")).distinct()
        total = alvo.count()
        print(f"\n=== {nome}: {total} CNPJs distintos ===")
        (
            alvo.join(cad, on="k", how="left")
            .withColumn("resultado", F.coalesce(F.col("uf_cad"), F.lit("NAO_CASOU_OU_UF_NULA")))
            .groupBy("resultado")
            .count()
            .orderBy(F.desc("count"))
            .show(15, truncate=False)
        )

    spark.stop()


if __name__ == "__main__":
    main()
