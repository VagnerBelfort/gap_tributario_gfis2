#!/usr/bin/env python3
"""Diagnóstico da base APL_SISCOMEX (Oracle C3) para uso no gap tributário.

Responde três perguntas que decidem se o Siscomex substitui o MDIC como fonte
de importações:

  A) Cobertura temporal e volume — a base é o universo das DIs ou um subconjunto?
  B) Matriz UF_DESPACHO x UF_IMPORTADOR — quanto de Itaqui é trânsito?
  C) Valor CIF (base de cálculo do II) por ano para importador MA.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/diagnostico_siscomex.py
"""

import os

from pyspark.sql import SparkSession

JDBC_URL = "jdbc:oracle:thin:@10.1.1.132:1521/cent"
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit(
        "Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.\n"
        "  export SISCOMEX_ORACLE_PASSWORD='...'"
    )
DRIVER = "oracle.jdbc.driver.OracleDriver"

# A chave da declaração é COMPOSTA: (NUM_DECL, SEQ_DECL).
# 57.574 linhas para 44.847 NUM_DECL distintos — join só por NUM_DECL infla somas.
JOIN_DECL_ITEM = (
    "d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL"
)

CONSULTAS = {
    "A_cobertura_por_ano": """
        SELECT EXTRACT(YEAR FROM TDS_DATA_DESEMBARACO)                  AS ano,
               COUNT(*)                                                 AS linhas,
               COUNT(DISTINCT TDS_NUM_DECL)                             AS dis_distintas,
               SUM(CASE WHEN TDS_UF_IMPORTADOR = 'MA' THEN 1 ELSE 0 END) AS di_importador_ma,
               SUM(CASE WHEN TDS_UF_DESPACHO   = 'MA' THEN 1 ELSE 0 END) AS di_despacho_ma
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY EXTRACT(YEAR FROM TDS_DATA_DESEMBARACO)
         ORDER BY 1
    """,
    "B_matriz_transito": """
        SELECT TDS_UF_DESPACHO           AS uf_despacho,
               TDS_UF_IMPORTADOR         AS uf_importador,
               COUNT(*)                  AS qtd,
               SUM(TDS_TOTAL_MERCADORIA) AS total_mercadoria
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         WHERE (TDS_UF_DESPACHO = 'MA' OR TDS_UF_IMPORTADOR = 'MA')
           AND EXTRACT(YEAR FROM TDS_DATA_DESEMBARACO) BETWEEN 2019 AND 2025
         GROUP BY TDS_UF_DESPACHO, TDS_UF_IMPORTADOR
         ORDER BY 3 DESC
    """,
    "C_cif_por_ano_importador_ma": f"""
        SELECT EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) AS ano,
               COUNT(DISTINCT d.TDS_NUM_DECL)            AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II)             AS cif_brl,
               SUM(i.TDI_VALOR_DEVIDO_ICMS)              AS icms_devido_brl
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i ON {JOIN_DECL_ITEM}
         WHERE d.TDS_UF_IMPORTADOR = 'MA'
         GROUP BY EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO)
         ORDER BY 1
    """,
    "D_situacao_e_tipo": """
        SELECT TDS_SITUACAO   AS situacao,
               TDS_TIPO_DECL  AS tipo_decl,
               COUNT(*)       AS qtd
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_SITUACAO, TDS_TIPO_DECL
         ORDER BY 3 DESC
    """,
}


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


def main():
    spark = SparkSession.builder.appName("Diagnostico Siscomex").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for nome, sql in CONSULTAS.items():
        print("\n" + "=" * 70)
        print(f"  {nome}")
        print("=" * 70)
        try:
            ler(spark, sql).show(200, truncate=False)
        except Exception as exc:  # diagnóstico: seguir mesmo se uma query falhar
            print(f"  FALHOU: {str(exc)[:400]}")

    spark.stop()


if __name__ == "__main__":
    main()
