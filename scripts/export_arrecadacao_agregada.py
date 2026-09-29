#!/usr/bin/env python3
"""Exporta a arrecadação de ICMS da gold, agregada por ano × trimestre.

O `ArrecadacaoExtractor` só soma as parcelas de ICMS filtrando por `per_aaaa` e
`per_nro_trimestre`. Agregar antes dá exatamente as mesmas somas e troca ~1,5 GB
de linhas por contribuinte por algumas dezenas de linhas, sem CNPJ.

Roda no cluster e grava um parquet no HDFS; traga com `hdfs dfs -get`.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    /gfis2/pipeline/gap_tributario/export_arrecadacao_agregada.py \
    --saida-hdfs /tmp/gap_tributario_g_arrecadacao_agregada
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABELA = "gfis2_ouro.g_arrecadacao"
ANO_INICIAL = 2019

# Mesma lista de `extractors/arrecadacao.py::_COLUNAS_ICMS`.
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--saida-hdfs", required=True, help="Diretório parquet no HDFS")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Export g_arrecadacao agregada")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tabela = spark.table(TABELA)
    existentes = set(tabela.columns)
    faltando = [c for c in COLUNAS_ICMS if c not in existentes]
    if faltando:
        print(f"\n=== COLUNAS AUSENTES NA GOLD: {faltando} ===")
    colunas = [c for c in COLUNAS_ICMS if c in existentes]

    agregado = (
        tabela.filter(F.col("per_aaaa") >= ANO_INICIAL)
        .groupBy(
            F.col("per_aaaa").cast("int").alias("per_aaaa"),
            F.col("per_nro_trimestre").cast("int").alias("per_nro_trimestre"),
        )
        .agg(
            F.count("*").alias("linhas_origem"),
            *[F.sum(F.col(c).cast("double")).alias(c) for c in colunas],
        )
        .orderBy("per_aaaa", "per_nro_trimestre")
    )

    print("\n=== ICMS POR ANO × TRIMESTRE (R$ mi) ===")
    agregado.select(
        "per_aaaa",
        "per_nro_trimestre",
        "linhas_origem",
        F.round(sum(F.coalesce(F.col(c), F.lit(0.0)) for c in colunas) / 1e6, 1).alias("icms_mi"),
    ).show(100, truncate=False)

    agregado.coalesce(1).write.mode("overwrite").parquet(args.saida_hdfs)
    print(f"\nParquet agregado escrito em hdfs://{args.saida_hdfs}")
    spark.stop()


if __name__ == "__main__":
    main()
