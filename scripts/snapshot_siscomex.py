#!/usr/bin/env python3
"""Materializa o snapshot agregado de importações do Siscomex (Oracle C3).

Roda no cluster da SEFAZ, onde o Oracle e o cadastro de contribuintes são
alcançáveis. Exporta um CSV AGREGADO — sem CNPJ, sem nome de importador, sem
nível de DI — para consumo pelo `gap_tributario` fora da rede da SEFAZ.

Regras de negócio, todas apuradas empiricamente (diagnósticos 1-3):

- Chave da declaração é COMPOSTA: (NUM_DECL, SEQ_DECL). Join só por NUM_DECL
  multiplica linhas.
- Retificação: cada versão da DI é uma linha. Vale a ÚLTIMA seq de cada DI.
- TDS_SITUACAO 'N' NÃO é cancelamento: essas DIs desembaraçam e pagam II/IPI
  em proporção comparável às 'S'. S, N e nulo entram. Confirmado pela SEFAZ-MA
  em 24/08/2026: o campo vem da Receita Federal e não é usado nas procedures
  da casa — "pode ser desconsiderado".
- TDS_TIPO_DECL: as DIs de situação nula têm tipo nulo. Aceita '01' e nulo,
  exclui os demais tipos (não-consumo), que somam 0,17% do valor.
- Valor: TDI_VALOR_BASE_CALC_II = base de cálculo do II = valor aduaneiro (CIF).
- UF do importador nula: resolvida por `b_contribuinte.uf_icms` via CNPJ. Isso
  recuperou 2.812 DIs maranhenses (R$ 7,71 bi); restaram 30 sem atribuição. O
  CNPJ é usado só aqui dentro e nunca é exportado.

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/snapshot_siscomex.py \
    --saida /gfis2/pipeline/gap_tributario/siscomex_importacoes.csv
"""

import argparse
import csv
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

# Uma linha por item da última versão de cada DI, já com o CIF e o CNPJ.
_SQL_ITENS = """
    WITH ultima AS (
        SELECT TDS_NUM_DECL, MAX(TDS_SEQ_DECL) AS SEQ
          FROM APL_SISCOMEX.TAB_DECL_SISCOMEX
         GROUP BY TDS_NUM_DECL
    )
    SELECT d.TDS_NUM_DECL                                AS num_decl,
           EXTRACT(YEAR  FROM d.TDS_DATA_DESEMBARACO)    AS ano,
           TO_NUMBER(TO_CHAR(d.TDS_DATA_DESEMBARACO, 'Q')) AS trimestre,
           d.TDS_UF_DESPACHO                             AS uf_despacho,
           d.TDS_UF_IMPORTADOR                           AS uf_importador,
           d.TDS_CNPJ                                    AS cnpj,
           d.TDS_TIPO_DECL                               AS tipo_decl,
           d.TDS_SITUACAO                                AS situacao,
           FLOOR(i.TDI_TNM_COD_NCM / 1000000)            AS capitulo_ncm,
           i.TDI_VALOR_BASE_CALC_II                      AS cif
      FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
      JOIN ultima u
        ON d.TDS_NUM_DECL = u.TDS_NUM_DECL AND d.TDS_SEQ_DECL = u.SEQ
      JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
        ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
     WHERE d.TDS_DATA_DESEMBARACO IS NOT NULL
       AND (d.TDS_TIPO_DECL IS NULL OR d.TDS_TIPO_DECL = '01')
"""


def _norm_cnpj(coluna):
    """CNPJ como string de 14 dígitos, sem '.0' de decimal e com zeros à esquerda."""
    limpo = F.regexp_replace(coluna.cast("string"), r"\.0+$", "")
    return F.lpad(limpo, 14, "0")


def main():
    parser = argparse.ArgumentParser(description="Snapshot agregado de importações Siscomex")
    parser.add_argument("--saida", required=True, help="Caminho do CSV de saída")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Snapshot Siscomex Importacoes")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    itens = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", _SQL_ITENS)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .option("fetchsize", "5000")
        .load()
    )

    # --- Resolução da UF nula pelo cadastro de contribuintes -----------------
    # A UF do contribuinte PJ está em `uf_icms` (preenchida em 4,646 mi das
    # 4,648 mi de linhas com CNPJ). NÃO usar `uf`: ela é nula em 100% das linhas
    # com CNPJ — só vale para pessoa física.
    #
    # Validação do método (debug_cnpj_cadastro.py v3): dos 797 CNPJs que o
    # Siscomex marca como MA, os 673 que casaram vieram todos como MA — nenhum
    # falso positivo. Dos 855 marcados como outra UF, o cadastro devolveu a UF
    # correta na maioria e disse MA em 12 casos (~9%), provavelmente inscrição
    # estadual de substituto tributário sediado fora. É a margem de erro do
    # método, e ela empurra levemente para cima.
    # Um CNPJ pode ter mais de uma inscrição estadual e, com isso, mais de uma
    # uf_icms. dropDuplicates escolheria uma arbitrariamente, e o snapshot
    # deixaria de ser reprodutível — o golden mudaria sem o dado mudar. Aqui,
    # CNPJ com UFs conflitantes fica sem atribuição (vira NI), que é o
    # comportamento honesto: não sabemos qual é o domicílio.
    cadastro = (
        spark.table(TABELA_CADASTRO)
        .filter(F.col("cnpj").isNotNull() & F.col("uf_icms").isNotNull())
        .select(
            _norm_cnpj(F.col("cnpj")).alias("cnpj_norm"),
            F.trim(F.col("uf_icms")).alias("uf_cadastro"),
        )
        .distinct()
        .groupBy("cnpj_norm")
        .agg(
            F.min("uf_cadastro").alias("uf_cadastro"),
            F.countDistinct("uf_cadastro").alias("n_ufs"),
        )
        .filter(F.col("n_ufs") == 1)
        .drop("n_ufs")
    )

    itens = itens.withColumn("cnpj_norm", _norm_cnpj(F.col("cnpj")))
    # nullif: UF em branco (CHAR do Oracle preenchido com espaços) sobreviveria
    # ao coalesce como string vazia e a linha não cairia nem em MA nem em NI —
    # sumiria do total e do teto de sensibilidade. Não ocorre nos dados atuais,
    # mas é barato garantir.
    resolvido = itens.join(cadastro, on="cnpj_norm", how="left").withColumn(
        "uf_final",
        F.coalesce(
            F.nullif(F.trim(F.col("uf_importador")), F.lit("")),
            F.nullif(F.trim(F.col("uf_cadastro")), F.lit("")),
            F.lit("NI"),
        ),
    )

    # Diagnóstico: quanto o cadastro resolveu do que estava nulo. 'NI' aqui
    # significa exclusivamente "CNPJ não encontrado no cadastro" — não confundir
    # com a string literal 'NI' que a coluna `uf` usa para pessoa física.
    print("\n=== RESOLUCAO DA UF NULA PELO CADASTRO ===")
    (
        resolvido.filter(F.nullif(F.trim(F.col("uf_importador")), F.lit("")).isNull())
        .groupBy("uf_final")
        .agg(F.countDistinct("num_decl").alias("dis"), F.sum("cif").alias("cif"))
        .orderBy(F.desc("cif"))
        .show(30, truncate=False)
    )

    # --- Agregado exportável: sem CNPJ, sem nível de DI ----------------------
    agregado = (
        resolvido.groupBy("ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_final")
        .agg(
            F.countDistinct("num_decl").alias("dis"),
            F.sum("cif").alias("cif_brl"),
        )
        .orderBy("ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_final")
    )

    print("\n=== TOTAL POR ANO, IMPORTADOR MA ===")
    (
        agregado.filter(F.col("uf_final") == "MA")
        .groupBy("ano")
        .agg(F.sum("dis").alias("dis"), (F.sum("cif_brl") / 1e9).alias("cif_bi"))
        .orderBy("ano")
        .show(30, truncate=False)
    )

    # A origem tem datas de desembaraço com ano digitado errado (18, 202, 203...).
    # São ~R$ 0,04 bi e nunca seriam selecionadas por um período real, mas sujam
    # o arquivo entregue. Descarta com contagem explícita — nada de corte mudo.
    descartadas = agregado.filter((F.col("ano") < 1997) | (F.col("ano") > 2030))
    n_descartadas = descartadas.count()
    if n_descartadas:
        print(f"\n=== ANOS INVÁLIDOS DESCARTADOS ({n_descartadas} linhas) ===")
        descartadas.groupBy("ano").agg(
            F.sum("dis").alias("dis"), F.sum("cif_brl").alias("cif")
        ).orderBy("ano").show(30, truncate=False)
    agregado = agregado.filter((F.col("ano") >= 1997) & (F.col("ano") <= 2030))

    linhas = agregado.collect()
    with open(args.saida, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(
            ["ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_importador", "dis", "cif_brl"]
        )
        for r in linhas:
            writer.writerow(
                [
                    int(r["ano"]) if r["ano"] is not None else "",
                    int(r["trimestre"]) if r["trimestre"] is not None else "",
                    int(r["capitulo_ncm"]) if r["capitulo_ncm"] is not None else "",
                    r["uf_despacho"] or "",
                    r["uf_final"] or "",
                    int(r["dis"]),
                    f"{float(r['cif_brl'] or 0):.2f}",
                ]
            )

    print(f"\nSnapshot escrito em {args.saida} ({len(linhas)} linhas agregadas)")
    spark.stop()


if __name__ == "__main__":
    main()
