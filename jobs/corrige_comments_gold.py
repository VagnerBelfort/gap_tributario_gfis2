#!/usr/bin/env python3
"""Corrige os COMMENTs desatualizados das g_gap_* da gfis2_ouro. Roda uma vez.

Só metadado: ALTER TABLE ... ALTER COLUMN ... COMMENT não mexe em coluna, tipo,
ordem nem dado, e o painel não enxerga a diferença. Fica fora do job de carga
para que a carga nunca toque em metadado de produção.

Sem `--aplicar`, só imprime os comandos.

  ./gap_tributario/run_gap_job.sh corrige_comments_gold.py [--aplicar]
"""

import argparse

from pyspark.sql import SparkSession

DATABASE = "gfis2_ouro"

# (tabela, coluna, comentário novo). Os antigos descreviam importações como
# FOB × PTAX do MDIC e listavam fontes que não existem mais.
COMENTARIOS = [
    ("g_gap_resultado", "importacoes",
     "Importações CIF por domicílio fiscal do importador (Siscomex: DI + DUIMP) — R$ milhões"),
    ("g_gap_resultado", "fonte_imp", "siscomex | mdic_bruto | manual"),
    ("g_gap_resultado", "flg_estimado", "VAB estimado — reservado; hoje sempre false"),
    ("g_gap_resultado", "versao_engine", "gap_tributario-cli-<data do cálculo>"),
    ("g_gap_resultado", "id_execucao", "CARGA_CLI_<data do cálculo> — CSVs do --exportar-ouro"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--aplicar", action="store_true", help="executa os ALTER TABLE")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("gap_corrige_comments_gold")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    for tabela, coluna, comentario in COMENTARIOS:
        texto = comentario.replace("'", "\\'")
        sql = f"ALTER TABLE {DATABASE}.{tabela} ALTER COLUMN {coluna} COMMENT '{texto}'"
        print(sql)
        if args.aplicar:
            spark.sql(sql)

    if not args.aplicar:
        print("\n(nada aplicado: rode com --aplicar)")
    spark.stop()


if __name__ == "__main__":
    main()
