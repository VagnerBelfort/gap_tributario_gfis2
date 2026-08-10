#!/usr/bin/env python3
"""Diagnóstico 3: significado de TDS_SITUACAO e resolução das UFs nulas.

Fecha os dois filtros que faltam antes de reescrever o extractor:

  I  — perfil das DIs por situação (tributos pagos, data de desembaraço).
       DI cancelada não recolhe II/IPI: se 'N' tiver tributo zerado, 'N' sai.
  J  — registro de entrega por situação. Sinal fraco (a tabela cobre ~11% das
       DIs), serve só para corroborar I.
  K  — 'N' é versão superada por retificação? Compara a situação em todas as
       linhas contra a situação apenas na última seq de cada DI.
  L  — quanto das UFs nulas o nível de item (TDI_UF_IMPORTADOR) resolve, e para
       quais UFs, no universo despachado no MA.
  M  — quantos CNPJs distintos sobram sem UF, para dimensionar o join com o
       cadastro de contribuintes.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/diagnostico_siscomex_3.py
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

CTE_ULTIMA_VERSAO = """
    WITH ultima AS (
        SELECT TDS_NUM_DECL, MAX(TDS_SEQ_DECL) AS SEQ
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_NUM_DECL
    )
"""

CONSULTAS = {
    # Sem join com item: TDS_TOTAL_MERCADORIA e os tributos ficam no nível certo.
    "I_perfil_por_situacao": """
        SELECT NVL(TDS_SITUACAO, '(nulo)')                                  AS situacao,
               COUNT(*)                                                     AS linhas,
               COUNT(DISTINCT TDS_NUM_DECL)                                 AS dis,
               SUM(CASE WHEN TDS_DATA_DESEMBARACO IS NULL THEN 1 ELSE 0 END) AS sem_data_desemb,
               SUM(TDS_VALOR_II_PAGO)                                       AS ii_pago,
               SUM(TDS_VALOR_IPI_PAGO)                                      AS ipi_pago,
               SUM(TDS_TOTAL_MERCADORIA)                                    AS total_mercadoria,
               SUM(CASE WHEN TDS_VALOR_II_PAGO > 0 THEN 1 ELSE 0 END)       AS com_ii_pago
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY NVL(TDS_SITUACAO, '(nulo)')
         ORDER BY 3 DESC
    """,
    "J_registro_entrega_por_situacao": """
        SELECT NVL(d.TDS_SITUACAO, '(nulo)')      AS situacao,
               COUNT(DISTINCT d.TDS_NUM_DECL)     AS dis,
               COUNT(DISTINCT r.TRE_TDS_NUM_DECL) AS dis_com_registro_entrega
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          LEFT JOIN APL_SISCOMEX.TAB_REGISTRO_ENTREGA r
            ON d.TDS_NUM_DECL = r.TRE_TDS_NUM_DECL
           AND d.TDS_SEQ_DECL = r.TRE_TDS_SEQ_DECL
         GROUP BY NVL(d.TDS_SITUACAO, '(nulo)')
         ORDER BY 2 DESC
    """,
    # Se 'N' for versão superada, ela quase desaparece ao ficar só a última seq.
    "K_situacao_na_ultima_versao": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT NVL(d.TDS_SITUACAO, '(nulo)')  AS situacao,
               COUNT(DISTINCT d.TDS_NUM_DECL) AS dis_ultima_versao
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
         GROUP BY NVL(d.TDS_SITUACAO, '(nulo)')
         ORDER BY 2 DESC
    """,
    # Das DIs despachadas no MA sem UF na declaração, para onde o item aponta?
    "L_uf_nula_resolvida_pelo_item": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT NVL(i.TDI_UF_IMPORTADOR, '(ainda nulo)') AS uf_pelo_item,
               COUNT(DISTINCT d.TDS_NUM_DECL)           AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II)            AS cif
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_IMPORTADOR IS NULL
           AND d.TDS_UF_DESPACHO = 'MA'
         GROUP BY NVL(i.TDI_UF_IMPORTADOR, '(ainda nulo)')
         ORDER BY 3 DESC NULLS LAST
    """,
    # O que sobrar depois do item precisa do cadastro de contribuintes via CNPJ.
    "M_cnpjs_sem_uf_apos_item": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT COUNT(DISTINCT d.TDS_CNPJ)     AS cnpjs_distintos,
               COUNT(DISTINCT d.TDS_NUM_DECL) AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II)  AS cif
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_IMPORTADOR IS NULL
           AND i.TDI_UF_IMPORTADOR IS NULL
           AND d.TDS_UF_DESPACHO = 'MA'
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
    spark = SparkSession.builder.appName("Diagnostico Siscomex 3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for nome, sql in CONSULTAS.items():
        print("\n" + "=" * 70)
        print(f"  {nome}")
        print("=" * 70)
        try:
            ler(spark, sql).show(60, truncate=False)
        except Exception as exc:
            print(f"  FALHOU: {str(exc)[:400]}")

    spark.stop()


if __name__ == "__main__":
    main()
