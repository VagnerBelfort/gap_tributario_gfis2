#!/usr/bin/env python3
"""Diagnóstico 2 da DUIMP: a situação 5 anda? A CARGA é legível?

A SEFAZ informou (28/09/2026) que a situação 5 é "Registrada. Aguardando
Resultado da Análise de Risco" e que a data de chegada está em
APL_PUCOMEX.CARGA.DATACHEGADA. Este script:

  A) testa por quais caminhos a CARGA (e a SituacaoDuimp) são legíveis para o
     WEB_GFIS2, no ARMA e no CENTRAL, inclusive pelo dblink CENTRAL;
  B) refaz o valor por mês × situação (uma linha por DUIMP, importador MA),
     para comparar com a leitura de 23/09 e ver se a situação 5 migrou;
  C) se a CARGA for legível, mostra o esquema e cruza DATACHEGADA com a
     situação: DUIMP na situação 5 com carga chegada é importação que ocorreu.

Só SELECT. Saída apenas agregada.

Uso (no cluster):
  export SISCOMEX_ORACLE_PASSWORD='...'
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/diagnostico_duimp_carga.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

URLS = {
    "arma": "jdbc:oracle:thin:@10.1.1.132:1521/arma",
    "cent": "jdbc:oracle:thin:@10.1.1.132:1521/cent",
}
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit(
        "Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.\n"
        "  export SISCOMEX_ORACLE_PASSWORD='...'"
    )
DRIVER = "oracle.jdbc.driver.OracleDriver"

CANDIDATOS = {
    "CARGA": ["CARGA", "APL_PUCOMEX.CARGA", "APL_PUCOMEX.CARGA@CENTRAL"],
    "SITUACAO": [
        "SITUACAODUIMP",
        "APL_PUCOMEX.SITUACAODUIMP",
        "APL_PUCOMEX.SITUACAODUIMP@CENTRAL",
    ],
    "DUIMP": ["DUIMP", "APL_PUCOMEX.DUIMP@CENTRAL"],
}

VL_DESC = "VLMERCADORIALOCALDESCARGAREAL"


def ler(spark, banco, sql):
    return (
        spark.read.format("jdbc")
        .option("url", URLS[banco])
        .option("query", sql)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .option("fetchsize", "5000")
        .load()
    )


def _erro_curto(exc):
    texto = str(exc)
    for linha in texto.splitlines():
        if "ORA-" in linha:
            return linha.strip()[:160]
    return texto.splitlines()[0][:160] if texto else repr(exc)


def primeiro_legivel(spark, objeto):
    """Testa cada caminho do objeto em cada banco; devolve o primeiro que lê."""
    achado = None
    for banco in URLS:
        for nome in CANDIDATOS[objeto]:
            try:
                n = ler(spark, banco, f"SELECT COUNT(*) FROM {nome}").collect()[0][0]
                print(f"  OK    {banco:5s} {nome:40s} {int(n)} linhas")
                if achado is None:
                    achado = (banco, nome)
            except Exception as exc:  # noqa: BLE001 — diagnóstico: reporta e segue
                print(f"  FALHA {banco:5s} {nome:40s} {_erro_curto(exc)}")
    return achado


def main():
    spark = SparkSession.builder.appName("Diagnostico DUIMP CARGA").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== A) CAMINHOS DE LEITURA ===")
    caminhos = {obj: primeiro_legivel(spark, obj) for obj in CANDIDATOS}
    print(f"\nEscolhidos: {caminhos}")

    print("\n=== A2) SINÔNIMOS VISÍVEIS COM PUCOMEX (arma) ===")
    try:
        ler(
            spark,
            "arma",
            "SELECT OWNER, SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK FROM ALL_SYNONYMS "
            "WHERE TABLE_OWNER = 'APL_PUCOMEX' OR SYNONYM_NAME LIKE '%DUIMP%' "
            "OR SYNONYM_NAME LIKE '%CARGA%'",
        ).show(50, truncate=False)
    except Exception as exc:  # noqa: BLE001
        print(f"  FALHA {_erro_curto(exc)}")

    # --- B) DUIMP de novo, uma linha por DUIMP, importador MA ---------------
    banco_d, nome_d = caminhos["DUIMP"] or ("arma", "DUIMP")
    duimp = ler(spark, banco_d, f"SELECT * FROM {nome_d} WHERE STVIGENTE = 'S'")
    janela = Window.partitionBy("NUMERODUIMP").orderBy(
        F.desc("VERSAODECLARACAO"), F.desc("IDDUIMP")
    )
    unico = (
        duimp.withColumn("rk", F.row_number().over(janela))
        .filter("rk = 1")
        .drop("rk")
        .filter(F.col("IDUFIMPORTADOR") == 10)
        .withColumn("mes", F.date_format("DATAHORAREGISTRO", "yyyy-MM"))
        .withColumn("sit", F.col("IDSITUACAODUIMP").cast("int"))
        .cache()
    )

    print("\n=== B) R$ MI POR MÊS DE REGISTRO × SITUAÇÃO (comparar com 23/09) ===")
    unico.groupBy("mes").pivot("sit").agg(F.round(F.sum(VL_DESC) / 1e6, 1)).orderBy(
        "mes"
    ).show(50)
    print("Quantidade de DUIMPs por mês × situação:")
    unico.groupBy("mes").pivot("sit").count().orderBy("mes").show(50)
    print("Total por situação:")
    unico.groupBy("sit").agg(
        F.count("*").alias("duimps"),
        F.round(F.sum(VL_DESC) / 1e6, 1).alias("desc_brl_mi"),
        F.min("DATAHORAREGISTRO").alias("registro_min"),
        F.max("DATAHORAREGISTRO").alias("registro_max"),
        F.max("DATAHORAREGISTROVERSAOVIGENTE").alias("versao_vigente_max"),
    ).orderBy("sit").show(30, truncate=False)

    # --- C) CARGA ------------------------------------------------------------
    if caminhos["CARGA"] is None:
        print("\n=== C) CARGA não é legível por nenhum caminho: pedir sinônimo à SEFAZ ===")
        spark.stop()
        return

    banco_c, nome_c = caminhos["CARGA"]
    carga = ler(spark, banco_c, f"SELECT * FROM {nome_c}")
    print(f"\n=== C) ESQUEMA DA CARGA ({banco_c} {nome_c}) ===")
    for campo in carga.schema.fields:
        print(f"  {campo.name:45s} {campo.dataType.simpleString()}")

    chave = next((c for c in ("IDDUIMP", "NUMERODUIMP") if c in carga.columns), None)
    if chave is None or "DATACHEGADA" not in carga.columns:
        print(f"\nSem chave óbvia para a DUIMP ou sem DATACHEGADA (chave={chave}). Ver esquema.")
        spark.stop()
        return

    print(f"\nChave usada: {chave}")
    carga_por_duimp = carga.groupBy(chave).agg(
        F.count("*").alias("cargas"),
        F.min("DATACHEGADA").alias("chegada"),
    )
    print("Cargas por DUIMP:")
    carga_por_duimp.groupBy("cargas").count().orderBy("cargas").show()

    cruz = unico.join(carga_por_duimp, chave, "left").withColumn(
        "tem_chegada", F.col("chegada").isNotNull()
    )
    print("\n=== C2) CHEGADA × SITUAÇÃO ===")
    cruz.groupBy("sit", "tem_chegada").agg(
        F.count("*").alias("duimps"),
        F.round(F.sum(VL_DESC) / 1e6, 1).alias("desc_brl_mi"),
        F.round(F.avg(F.datediff("chegada", "DATAHORAREGISTRO")), 1).alias("dias_reg_ate_chegada"),
    ).orderBy("sit", "tem_chegada").show(50)

    print("\n=== C3) R$ MI POR TRIMESTRE: REGISTRO × CHEGADA ===")
    cruz.withColumn(
        "tri_registro", F.concat_ws("-T", F.year("DATAHORAREGISTRO").cast("string"), F.quarter("DATAHORAREGISTRO").cast("string"))
    ).withColumn(
        "tri_chegada", F.concat_ws("-T", F.year("chegada").cast("string"), F.quarter("chegada").cast("string"))
    ).groupBy("tri_registro", "tri_chegada").agg(
        F.round(F.sum(VL_DESC) / 1e6, 1).alias("desc_brl_mi")
    ).orderBy("tri_registro", "tri_chegada").show(100)

    spark.stop()


if __name__ == "__main__":
    main()
