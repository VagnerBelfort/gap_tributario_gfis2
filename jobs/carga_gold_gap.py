#!/usr/bin/env python3
"""Carga do Gap Tributário no Impala: CSVs do CLI → gfis2_dev → produção.

Dois passos, sempre nesta ordem:

  carregar   Lê os CSVs que `python -m gap_tributario --exportar-ouro` gerou
             (já copiados para o HDFS) e grava as três g_gap_* no database de
             validação. Nunca grava na gfis2_ouro.
  promover   Depois do checklist, copia do dev para a produção, tabela por
             tabela: g_gap_* → gfis2_ouro; b_siscomex_di, b_duimp,
             b_duimp_carga → gfis2_bronze; s_gap_importacoes → gfis2_prata.
             Vai para a produção exatamente o que foi validado.

Este job não faz conta: os números vêm do MotorVRR, pelo CLI. As travas estão
em `carga_gold_regras.py` (testadas no pytest local):

- aborta se a estrutura da tabela ou o cabeçalho do CSV divergir do schema de
  produção (o painel da SEFAZ lê essas tabelas);
- aborta se aparecer um valor categórico que a produção não tem;
- avisa, sem abortar, se o ICMS do CSV se afastar mais de 0,5% da
  g_arrecadacao de agora, ou se as importações do CSV não baterem com a soma
  do MA na s_gap_importacoes.

Python 3.6.8 (driver do cluster). Uso, a partir da raiz do pipeline:

  ./gap_tributario/run_gap_job.sh carga_gold_gap.py carregar \\
      --csv-dir /tmp/gap_tributario/ouro_20261001
  ./gap_tributario/run_gap_job.sh carga_gold_gap.py promover
"""

import argparse
import sys
from decimal import Decimal

from carga_gold_regras import (
    CATEGORICAS,
    COLUNAS_ICMS,
    SCHEMA_OURO,
    TOLERANCIA_ICMS_PCT,
    TOLERANCIA_IMPORTACOES_PCT,
    avisos_divergencia,
    diferencas_colunas,
    valores_ineditos,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PRODUCAO_OURO = "gfis2_ouro"
TABELA_ARRECADACAO = "gfis2_ouro.g_arrecadacao"

# Tabela do dev → database de produção que a recebe na promoção.
DESTINO_BRONZE_PRATA = {
    "b_siscomex_di": "gfis2_bronze",
    "b_duimp": "gfis2_bronze",
    "b_duimp_carga": "gfis2_bronze",
    "s_gap_importacoes": "gfis2_prata",
}


class CargaAbortada(Exception):
    pass


def _schema_ddl(tabela):
    return ", ".join(f"{nome} {tipo}" for nome, tipo in SCHEMA_OURO[tabela])


def _exigir(problemas, contexto):
    if problemas:
        raise CargaAbortada(contexto + ":\n  " + "\n  ".join(problemas))


def _distintos(df, colunas):
    return {c: {r[0] for r in df.select(c).distinct().collect()} for c in colunas}


def _conferir_tabela(spark, tabela_completa, tabela):
    _exigir(
        diferencas_colunas(SCHEMA_OURO[tabela], spark.table(tabela_completa).dtypes),
        f"{tabela_completa} não tem o schema de produção",
    )


def _conferir_categoricas(spark, df, tabela, rotulo):
    producao = _distintos(spark.table(f"{PRODUCAO_OURO}.{tabela}"), CATEGORICAS[tabela])
    _exigir(
        valores_ineditos(_distintos(df, CATEGORICAS[tabela]), producao),
        f"{rotulo} traz valor categórico que o painel não conhece",
    )


def _por_ano(linhas):
    return {int(r[0]): Decimal(str(r[1])) for r in linhas if r[1] is not None}


def _imprimir_avisos(titulo, avisos):
    print(f"\n=== {titulo} ===")
    if not avisos:
        print("  ok")
    for aviso in avisos:
        print(f"  AVISO {aviso}")


def carregar(spark, csv_dir, database, database_prata):
    if database == PRODUCAO_OURO:
        raise CargaAbortada(
            "carregar não grava na gfis2_ouro: carregue no dev, valide e use `promover`."
        )

    frames = {}
    for tabela in SCHEMA_OURO:
        caminho = f"{csv_dir.rstrip('/')}/{tabela}.csv"
        cabecalho = spark.read.option("header", True).csv(caminho).columns
        _exigir(
            diferencas_colunas([c for c, _ in SCHEMA_OURO[tabela]], cabecalho),
            f"cabeçalho de {caminho}",
        )
        df = (
            spark.read.option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", True)
            .option("mode", "FAILFAST")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(_schema_ddl(tabela))
            .csv(caminho)
        )
        _conferir_categoricas(spark, df, tabela, caminho)
        frames[tabela] = df

    resultado = frames["g_gap_resultado"]
    anos = [r[0] for r in resultado.select("ano").collect()]

    icms_csv = _por_ano(resultado.select("ano", "icms_arrecadado").collect())
    arrecadacao = spark.table(TABELA_ARRECADACAO)
    colunas = [c for c in COLUNAS_ICMS if c in arrecadacao.columns]
    icms_agora = _por_ano(
        arrecadacao.filter(F.col("per_aaaa").cast("int").isin(anos))
        .groupBy(F.col("per_aaaa").cast("int"))
        .agg(sum(F.coalesce(F.sum(F.col(c).cast("double")), F.lit(0.0)) for c in colunas) / 1e6)
        .collect()
    )
    _imprimir_avisos(
        f"ICMS do CSV × {TABELA_ARRECADACAO} (tolerância {TOLERANCIA_ICMS_PCT}%)",
        avisos_divergencia("ICMS", icms_csv, icms_agora, TOLERANCIA_ICMS_PCT),
    )

    prata = f"{database_prata}.s_gap_importacoes"
    if spark.catalog.tableExists(prata):
        imp_csv = _por_ano(resultado.select("ano", "importacoes").collect())
        imp_prata = _por_ano(
            spark.table(prata)
            .filter((F.col("uf_importador") == "MA") & F.col("ano").isin(anos))
            .groupBy("ano")
            .agg(F.sum("cif_brl") / 1e6)
            .collect()
        )
        _imprimir_avisos(
            f"Importações do CSV × soma MA de {prata}",
            avisos_divergencia("Importações", imp_csv, imp_prata, TOLERANCIA_IMPORTACOES_PCT),
        )
    else:
        _imprimir_avisos("Importações do CSV × prata", [f"{prata} não existe"])

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    for tabela, df in frames.items():
        destino = f"{database}.{tabela}"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {destino} ({_schema_ddl(tabela)}) USING iceberg"
        )
        _conferir_tabela(spark, destino, tabela)
        view = f"tmp_carga_{tabela}"
        df.createOrReplaceTempView(view)
        spark.sql(f"INSERT OVERWRITE TABLE {destino} SELECT * FROM {view}")
        print(f"[CARGA] {destino}: {spark.table(destino).count()} linhas")


def _snapshot_atual(spark, tabela_completa):
    linhas = spark.sql(
        f"SELECT snapshot_id FROM {tabela_completa}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return linhas[0][0] if linhas else None


def _gravar_tabela(spark, origem, destino):
    """Sobrescreve `destino` com `origem`, criando-o na primeira vez."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {destino} USING iceberg AS SELECT * FROM {origem} WHERE 1 = 0"
    )
    spark.sql(f"INSERT OVERWRITE TABLE {destino} SELECT * FROM {origem}")


def promover(spark, origem):
    # Confere tudo antes de gravar qualquer coisa: promoção pela metade
    # deixaria o painel com tabelas de cargas diferentes.
    for tabela in SCHEMA_OURO:
        _conferir_tabela(spark, f"{origem}.{tabela}", tabela)
        _conferir_tabela(spark, f"{PRODUCAO_OURO}.{tabela}", tabela)
        _conferir_categoricas(
            spark, spark.table(f"{origem}.{tabela}"), tabela, f"{origem}.{tabela}"
        )
    for tabela in DESTINO_BRONZE_PRATA:
        if not spark.catalog.tableExists(f"{origem}.{tabela}"):
            raise CargaAbortada(f"{origem}.{tabela} não existe: rode o snapshot_siscomex.py")

    print("\n=== SNAPSHOTS ANTERIORES (rollback) ===")
    for tabela in SCHEMA_OURO:
        destino = f"{PRODUCAO_OURO}.{tabela}"
        print(f"  {destino}: snapshot_id {_snapshot_atual(spark, destino)}")

    # Na ouro, só INSERT OVERWRITE: as tabelas existem e foram conferidas
    # acima, e nenhum DDL roda ali. Bronze e prata podem nascer nesta promoção.
    for tabela in SCHEMA_OURO:
        destino = f"{PRODUCAO_OURO}.{tabela}"
        spark.sql(f"INSERT OVERWRITE TABLE {destino} SELECT * FROM {origem}.{tabela}")
        print(f"[PROMOVIDO] {origem}.{tabela} → {destino}: {spark.table(destino).count()} linhas")
    for tabela, database in DESTINO_BRONZE_PRATA.items():
        destino = f"{database}.{tabela}"
        _gravar_tabela(spark, f"{origem}.{tabela}", destino)
        print(f"[PROMOVIDO] {origem}.{tabela} → {destino}: {spark.table(destino).count()} linhas")

    destinos = [(t, PRODUCAO_OURO) for t in SCHEMA_OURO] + list(DESTINO_BRONZE_PRATA.items())

    print("\nRode no Impala para enxergar a carga:")
    for tabela, database in destinos:
        print(f"  INVALIDATE METADATA {database}.{tabela};")
    print("\nRollback de uma tabela da ouro, se preciso:")
    print("  CALL spark_catalog.system.rollback_to_snapshot('gfis2_ouro.<tabela>', <snapshot_id>)")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="comando")
    p_carregar = sub.add_parser("carregar", help="CSVs do CLI → database de validação")
    p_carregar.add_argument("--csv-dir", required=True, help="diretório HDFS com os 3 CSVs")
    p_carregar.add_argument("--database", default="gfis2_dev")
    p_carregar.add_argument("--database-prata", default="gfis2_dev",
                            help="onde está a s_gap_importacoes desta extração")
    p_promover = sub.add_parser("promover", help="dev → produção, por cópia")
    p_promover.add_argument("--origem", default="gfis2_dev")
    args = parser.parse_args()
    if args.comando is None:
        parser.print_help()
        return 2

    spark = (
        SparkSession.builder.appName(f"gap_carga_gold_{args.comando}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.comando == "carregar":
            carregar(spark, args.csv_dir, args.database, args.database_prata)
        else:
            promover(spark, args.origem)
    except CargaAbortada as exc:
        print(f"\n*** CARGA ABORTADA *** {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
