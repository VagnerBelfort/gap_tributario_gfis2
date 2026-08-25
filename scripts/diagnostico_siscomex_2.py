#!/usr/bin/env python3
"""Diagnóstico 2 do APL_SISCOMEX: deduplicação de retificações e unidades.

O diagnóstico 1 mostrou ICMS devido na importação (2022) = R$ 13,1 bi, maior que
o ICMS total arrecadado pelo MA no ano (R$ 10,9 bi) — impossível. Hipótese: a
TAB_DECL_SISCOMEX guarda uma linha por VERSÃO da DI (TDS_SEQ_DECL = retificação),
e somar todas as versões multiplica os valores.

Este script mede o efeito da deduplicação e valida a unidade dos campos de valor.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/diagnostico_siscomex_2.py
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

# Última versão de cada DI: a retificação mais recente substitui as anteriores.
CTE_ULTIMA_VERSAO = """
    WITH ultima AS (
        SELECT TDS_NUM_DECL, MAX(TDS_SEQ_DECL) AS SEQ
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_NUM_DECL
    )
"""

CONSULTAS = {
    # Compara soma com e sem deduplicação. Se a hipótese estiver certa, a coluna
    # dedup cai para perto do valor plausível e a razão explica o fator de inflação.
    "E_efeito_da_deduplicacao": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) AS ano,
               COUNT(DISTINCT d.TDS_NUM_DECL)            AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II)             AS cif_dedup,
               SUM(i.TDI_VALOR_DEVIDO_ICMS)              AS icms_dedup
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_IMPORTADOR = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) BETWEEN 2019 AND 2025
         GROUP BY EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO)
         ORDER BY 1
    """,
    # TDS_TOTAL_MERCADORIA é USD (VMLE) ou BRL? Se USD, a razão CIF/TOTAL deve
    # ficar próxima da taxa de câmbio média do ano.
    "F_unidade_dos_valores": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) AS ano,
               SUM(d.TDS_TOTAL_MERCADORIA)               AS total_mercadoria,
               AVG(d.TDS_TAXA_CAMBIO)                    AS taxa_cambio_media,
               SUM(i.TDI_VALOR_BASE_CALC_II)             AS cif_base_ii
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_IMPORTADOR = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) BETWEEN 2021 AND 2023
         GROUP BY EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO)
         ORDER BY 1
    """,
    # Qual filtro de situação/tipo é o correto? Mede o peso de cada recorte em 2022.
    "G_2022_por_situacao_e_tipo": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT d.TDS_SITUACAO                AS situacao,
               d.TDS_TIPO_DECL               AS tipo_decl,
               COUNT(DISTINCT d.TDS_NUM_DECL) AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II) AS cif
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_IMPORTADOR = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) = 2022
         GROUP BY d.TDS_SITUACAO, d.TDS_TIPO_DECL
         ORDER BY 4 DESC NULLS LAST
    """,
    # O trânsito de Itaqui por capítulo NCM: valida (ou derruba) o filtro cap.27/31.
    "H_transito_por_capitulo_ncm": f"""
        {CTE_ULTIMA_VERSAO}
        SELECT FLOOR(i.TDI_TNM_COD_NCM / 1000000) AS capitulo_ncm,
               CASE WHEN d.TDS_UF_IMPORTADOR = 'MA' THEN 'MA'
                    WHEN d.TDS_UF_IMPORTADOR IS NULL THEN 'NULO'
                    ELSE 'OUTRA_UF' END          AS destino,
               COUNT(DISTINCT d.TDS_NUM_DECL)    AS dis,
               SUM(i.TDI_VALOR_BASE_CALC_II)     AS cif
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE d.TDS_UF_DESPACHO = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) = 2022
         GROUP BY FLOOR(i.TDI_TNM_COD_NCM / 1000000),
                  CASE WHEN d.TDS_UF_IMPORTADOR = 'MA' THEN 'MA'
                       WHEN d.TDS_UF_IMPORTADOR IS NULL THEN 'NULO'
                       ELSE 'OUTRA_UF' END
         ORDER BY 4 DESC NULLS LAST
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
    spark = SparkSession.builder.appName("Diagnostico Siscomex 2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for nome, sql in CONSULTAS.items():
        print("\n" + "=" * 70)
        print(f"  {nome}")
        print("=" * 70)
        try:
            ler(spark, sql).show(100, truncate=False)
        except Exception as exc:
            print(f"  FALHOU: {str(exc)[:400]}")

    spark.stop()


if __name__ == "__main__":
    main()
