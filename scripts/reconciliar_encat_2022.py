#!/usr/bin/env python3
"""Reconcilia o número de importações da referência ENCAT (R$ 21.924 mi, 2022).

A apresentação do 79º ENCAT (NEEF/SEFAZ-MA) usa Imp 2022 = R$ 21.924 mi e
descreve a fonte como "Siscomex — UF de destino/consumo". Nossa apuração pelo
mesmo Siscomex, com TDS_UF_IMPORTADOR e valor aduaneiro, dá R$ 39.704 mi.

A aritmética do slide mostra 21.924 = 4.200 × 5,22 — ou seja, o número de
origem é US$ 4,2 bi arredondado.

Este script varre as combinações plausíveis de (campo de UF) × (campo de valor)
× (filtro de situação) para 2022 e imprime cada total em R$ mi, para identificar
qual reproduz a referência. Não altera nada — é só leitura.

Uso:
  export SISCOMEX_ORACLE_PASSWORD='...'
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/reconciliar_encat_2022.py
"""

import os

from pyspark.sql import SparkSession

JDBC_URL = "jdbc:oracle:thin:@10.1.1.132:1521/cent"
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit("Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.")
DRIVER = "oracle.jdbc.driver.OracleDriver"

REFERENCIA_ENCAT_MI = 21924.0

CTE = """
    WITH ultima AS (
        SELECT TDS_NUM_DECL, MAX(TDS_SEQ_DECL) AS SEQ
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_NUM_DECL
    )
"""

# (rótulo, coluna de UF na declaração, coluna de valor, nível, filtro extra)
VARIANTES = [
    ("UF_IMPORTADOR | base_calc_II (nosso)", "d.TDS_UF_IMPORTADOR", "i.TDI_VALOR_BASE_CALC_II", "item", ""),
    ("UF_IMPORTADOR | base_calc_II | só S", "d.TDS_UF_IMPORTADOR", "i.TDI_VALOR_BASE_CALC_II", "item", "AND d.TDS_SITUACAO = 'S'"),
    ("UF_IMPORTADOR | base_calc_SEFAZ", "d.TDS_UF_IMPORTADOR", "i.TDI_BASE_CALC_SEFAZ", "item", ""),
    ("UF_IMPORTADOR | total_mercadoria", "d.TDS_UF_IMPORTADOR", "d.TDS_TOTAL_MERCADORIA", "decl", ""),
    ("UF_IMPORTADOR | total_mercadoria | só S", "d.TDS_UF_IMPORTADOR", "d.TDS_TOTAL_MERCADORIA", "decl", "AND d.TDS_SITUACAO = 'S'"),
    ("UF_CREDITO | base_calc_II", "d.TDS_UF_CREDITO", "i.TDI_VALOR_BASE_CALC_II", "item", ""),
    ("UF_CREDITO | total_mercadoria", "d.TDS_UF_CREDITO", "d.TDS_TOTAL_MERCADORIA", "decl", ""),
    ("UF_CONSIGNATARIO | base_calc_II", "d.TDS_UF_CONSIGNATARIO", "i.TDI_VALOR_BASE_CALC_II", "item", ""),
    ("UF_CONSIGNATARIO | total_mercadoria", "d.TDS_UF_CONSIGNATARIO", "d.TDS_TOTAL_MERCADORIA", "decl", ""),
    ("UF_DESPACHO | base_calc_II", "d.TDS_UF_DESPACHO", "i.TDI_VALOR_BASE_CALC_II", "item", ""),
]


def _sql(col_uf, col_valor, nivel, extra):
    """Monta a consulta. Nível 'decl' evita fan-out do valor da declaração."""
    if nivel == "decl":
        return f"""
        {CTE}
        SELECT SUM({col_valor}) AS total
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
         WHERE {col_uf} = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) = 2022
           AND (d.TDS_TIPO_DECL IS NULL OR d.TDS_TIPO_DECL = '01')
           {extra}
    """
    return f"""
        {CTE}
        SELECT SUM({col_valor}) AS total
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
          JOIN ultima u ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
          JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
            ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
         WHERE {col_uf} = 'MA'
           AND EXTRACT(YEAR FROM d.TDS_DATA_DESEMBARACO) = 2022
           AND (d.TDS_TIPO_DECL IS NULL OR d.TDS_TIPO_DECL = '01')
           {extra}
    """


def main():
    spark = SparkSession.builder.appName("Reconciliar ENCAT 2022").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\nReferência ENCAT 2022: R$ {REFERENCIA_ENCAT_MI:,.0f} mi (US$ 4,2 bi × 5,22)\n")
    print(f"{'variante':<45} {'R$ mi':>14} {'vs ref':>10}")
    print("-" * 72)

    for rotulo, col_uf, col_valor, nivel, extra in VARIANTES:
        try:
            df = (
                spark.read.format("jdbc")
                .option("url", JDBC_URL)
                .option("query", _sql(col_uf, col_valor, nivel, extra))
                .option("user", USER)
                .option("password", PASSWORD)
                .option("driver", DRIVER)
                .load()
            )
            total = df.collect()[0]["TOTAL"]
            if total is None:
                print(f"{rotulo:<45} {'(nulo)':>14} {'-':>10}")
                continue
            mi = float(total) / 1e6
            desvio = (mi / REFERENCIA_ENCAT_MI - 1) * 100
            marca = "  <<<" if abs(desvio) < 10 else ""
            print(f"{rotulo:<45} {mi:>14,.0f} {desvio:>9.1f}%{marca}")
        except Exception as exc:
            print(f"{rotulo:<45} FALHOU: {str(exc)[:60]}")

    print("\n'<<<' marca as variantes a menos de 10% da referência.")
    spark.stop()


if __name__ == "__main__":
    main()
