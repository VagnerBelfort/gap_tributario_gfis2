#!/usr/bin/env python3
"""Diagnóstico da DUIMP pelo sinônimo `DUIMP` no ARMA de produção.

Responde o que falta para a DUIMP entrar nas importações de 2026:
  A) esquema da tabela (há alguma data de desembaraço?)
  B) versões: STVIGENTE isola uma linha por NUMERODUIMP?
  C) distribuição de IDSITUACAODUIMP, INDICADORCUMPRIMENTOICMS e IDCANALSELECAO
  D) valor por trimestre de registro e razão descarga/embarque (CIF/FOB)
  E) IDUFIMPORTADOR × UF do cadastro (b_contribuinte.uf_icms via CNPJ)

Só SELECT. Saída apenas agregada: nenhum CNPJ, nome ou número de DUIMP é
impresso.

Uso (no cluster):
  export SISCOMEX_ORACLE_PASSWORD='...'
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/diagnostico_duimp_arma.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_URL = "jdbc:oracle:thin:@10.1.1.132:1521/arma"
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit(
        "Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.\n"
        "  export SISCOMEX_ORACLE_PASSWORD='...'"
    )
DRIVER = "oracle.jdbc.driver.OracleDriver"

TABELA_CADASTRO = "gfis2_bronze.b_contribuinte"

VL_EMB = "VLMERCADORIALOCALEMBARQUEREAL"
VL_DESC = "VLMERCADORIALOCALDESCARGAREAL"


def ler(spark, sql):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", sql)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .load()
    )


def _norm_cnpj(coluna):
    limpo = F.regexp_replace(coluna.cast("string"), r"\.0+$", "")
    return F.lpad(limpo, 14, "0")


def _somas(df):
    return [
        F.count("*").alias("linhas"),
        F.countDistinct("NUMERODUIMP").alias("duimps"),
        F.round(F.sum(VL_EMB) / 1e6, 1).alias("emb_brl_mi"),
        F.round(F.sum(VL_DESC) / 1e6, 1).alias("desc_brl_mi"),
        F.round(F.sum(VL_DESC) / F.sum(VL_EMB), 4).alias("desc_sobre_emb"),
    ]


def main():
    spark = SparkSession.builder.appName("Diagnostico DUIMP ARMA").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    duimp = ler(spark, "SELECT * FROM DUIMP").cache()

    print("\n=== A) ESQUEMA DA TABELA DUIMP ===")
    for campo in duimp.schema.fields:
        print(f"  {campo.name:45s} {campo.dataType.simpleString()}")

    print("\n=== B) VERSÕES ===")
    duimp.agg(
        F.count("*").alias("linhas"),
        F.countDistinct("NUMERODUIMP").alias("duimps_distintas"),
    ).show()
    duimp.groupBy("STVIGENTE").agg(*_somas(duimp)).orderBy("STVIGENTE").show()
    vig = duimp.filter(F.col("STVIGENTE") == "S")
    print("DUIMPs com mais de uma linha vigente:")
    print(vig.groupBy("NUMERODUIMP").count().filter("count > 1").count())
    print("DUIMPs sem nenhuma linha vigente:")
    print(
        duimp.select("NUMERODUIMP").distinct()
        .join(vig.select("NUMERODUIMP").distinct(), "NUMERODUIMP", "left_anti")
        .count()
    )

    print("\n=== C) CÓDIGOS (só linhas vigentes) ===")
    for col in ["IDSITUACAODUIMP", "INDICADORCUMPRIMENTOICMS", "IDCANALSELECAO"]:
        vig.groupBy(col).agg(*_somas(vig)).orderBy(col).show(50, truncate=False)

    print("\n=== D) POR TRIMESTRE DE REGISTRO (vigentes) ===")
    por_tri = vig.withColumn("ano", F.year("DATAHORAREGISTRO")).withColumn(
        "tri", F.quarter("DATAHORAREGISTRO")
    )
    por_tri.groupBy("ano", "tri").agg(*_somas(vig)).orderBy("ano", "tri").show(50)
    print("Por trimestre × situação:")
    por_tri.groupBy("ano", "tri", "IDSITUACAODUIMP").agg(*_somas(vig)).orderBy(
        "ano", "tri", "IDSITUACAODUIMP"
    ).show(200)

    print("\n=== E) UF: IDUFIMPORTADOR × CADASTRO (vigentes) ===")
    # Mesma regra do snapshot_siscomex.py: CNPJ com UFs conflitantes fica sem UF.
    cadastro = (
        spark.table(TABELA_CADASTRO)
        .filter(F.col("cnpj").isNotNull() & F.col("uf_icms").isNotNull())
        .select(
            _norm_cnpj(F.col("cnpj")).alias("cnpj14"),
            F.trim(F.col("uf_icms")).alias("uf_icms"),
        )
        .distinct()
        .groupBy("cnpj14")
        .agg(F.min("uf_icms").alias("uf_icms"), F.countDistinct("uf_icms").alias("n"))
        .filter(F.col("n") == 1)
        .drop("n")
    )
    com_uf = vig.withColumn("cnpj14", _norm_cnpj(F.col("CNPJIMPORTADOR"))).join(
        cadastro, "cnpj14", "left"
    )
    com_uf.groupBy("IDUFIMPORTADOR", "uf_icms").agg(*_somas(vig)).orderBy(
        F.desc("desc_brl_mi")
    ).show(50)

    print("\n=== F) AS 228 DUIMPs COM MAIS DE UMA LINHA VIGENTE ===")
    dup = vig.groupBy("NUMERODUIMP").agg(
        F.count("*").alias("n"),
        F.countDistinct("VERSAODECLARACAO").alias("versoes"),
        F.countDistinct("IDDUIMP").alias("ids"),
        F.countDistinct("IDSITUACAODUIMP").alias("situacoes"),
        F.countDistinct(VL_DESC).alias("valores"),
    ).filter("n > 1")
    dup.groupBy("n", "versoes", "ids", "situacoes", "valores").count().orderBy(
        F.desc("count")
    ).show(30)
    print("VERSAODECLARACAOVIGENTE bate com VERSAODECLARACAO? (vigentes)")
    vig.groupBy(
        (F.col("VERSAODECLARACAOVIGENTE").cast("int") == F.col("VERSAODECLARACAO"))
        .alias("bate")
    ).count().show()

    print("\n=== G) DEDUPLICADO (maior versão, depois maior IDDUIMP) — POR MÊS × SITUAÇÃO ===")
    from pyspark.sql.window import Window

    janela = Window.partitionBy("NUMERODUIMP").orderBy(
        F.desc("VERSAODECLARACAO"), F.desc("IDDUIMP")
    )
    unico = (
        vig.withColumn("rk", F.row_number().over(janela))
        .filter("rk = 1")
        .filter(F.col("IDUFIMPORTADOR") == 10)
        .withColumn("mes", F.date_format("DATAHORAREGISTRO", "yyyy-MM"))
    )
    unico.groupBy("mes").pivot("IDSITUACAODUIMP").agg(
        F.round(F.sum(VL_DESC) / 1e6, 1)
    ).orderBy("mes").show(50)
    unico.groupBy("mes").agg(*_somas(unico)).orderBy("mes").show(50)

    spark.stop()


if __name__ == "__main__":
    main()
