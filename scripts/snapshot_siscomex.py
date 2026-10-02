#!/usr/bin/env python3
"""Materializa o snapshot agregado de importações do Siscomex (Oracle C3).

Roda no cluster da SEFAZ, onde o Oracle e o cadastro de contribuintes são
alcançáveis. Exporta um CSV AGREGADO — sem CNPJ, sem nome de importador, sem
nível de DI — para consumo pelo `gap_tributario` fora da rede da SEFAZ.

Grava também, no Impala, o que leu e o que agregou:

- bronze (`--database-bronze`): `b_siscomex_di`, `b_duimp` e `b_duimp_carga`,
  cópias das tabelas de origem com TODAS as versões das declarações, só as
  colunas que este job usa, as chaves e o CNPJ. As regras abaixo (versão,
  data, tipo) rodam no Spark sobre essas cópias, então uma mudança de regra
  é reprocessamento, sem voltar ao Oracle.
- prata (`--database-prata`): `s_gap_importacoes`, o mesmo agregado do CSV,
  com `qtd_declaracoes` no lugar de `dis` e a data da extração em `dt_snapshot`.

Os dois databases saem em `gfis2_dev` por padrão. A produção recebe por cópia
do dev, depois da validação (`jobs/carga_gold_gap.py promover`).

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

DUIMP (Declaração Única de Importação, Portal Único) — substitui a DI a partir
de nov/2025. Lida do ARMA pelo sinônimo `DUIMP` (-> APL_PUCOMEX.DUIMP@CENTRAL),
criado pela SEFAZ-MA em 21/09/2026; o schema APL_PUCOMEX não é legível direto.
Regras apuradas no diagnóstico de 23/09/2026 (diagnostico_duimp_arma.py):

- STVIGENTE = 'S' NÃO isola uma linha por DUIMP: 228 DUIMPs têm mais de uma
  versão marcada vigente. Vale a maior VERSAODECLARACAO de cada NUMERODUIMP.
  Somar todas as versões inflava o total de R$ 12,9 bi para R$ 22,9 bi.
- Valor: VLMERCADORIALOCALDESCARGAREAL (mercadoria no local de descarga = CIF).
  A razão sobre o valor no embarque fica em ~1,05, a assinatura CIF/FOB da DI.
- Período: a data mais tardia entre DATAHORAREGISTRO e a DATACHEGADA da carga
  (APL_PUCOMEX.CARGA@CENTRAL, lida do ARMA). A SEFAZ-MA (TI) indicou
  DATACHEGADA como a data de liberação e confirmou o uso do dblink em
  28/09/2026. A DUIMP não tem data de desembaraço. A carga
  costuma chegar antes do registro (em média 9 dias), e aí vale o registro;
  quando chega depois, a importação só se completa na chegada. DATACHEGADA
  nula não significa importação que não aconteceu (há R$ 1,2 bi desembaraçados
  sem ela): vale o registro. Join pelo IDDUIMP da versão escolhida, que
  identifica a versão, não a declaração. Diagnóstico de 28/09/2026
  (diagnostico_duimp_carga.py): uma carga por DUIMP; a regra desloca
  ~R$ 357 mi de 2025-T4 para 2026-T1.
- UF: IDUFIMPORTADOR é o índice da UF em ordem alfabética do nome (10 = MA).
  Conferido contra o cadastro: 10 → MA em 100% dos CNPJs casados, 2 → AL.
  Nulo cai para o cadastro, como na DI.
- Situação (IDSITUACAODUIMP): todas entram. Na base aparecem 5, 6, 8, 10
  (registrada ou em conferência) e 11, 12, 13 (desembaraçadas); nenhuma
  cancelada (22, 23). A situação 5 ("aguardando análise de risco", ~R$ 2,6 bi)
  não anda na cópia da SEFAZ, e 76% dessas DUIMPs têm a carga chegada: são
  importações reais. Sem ela, o 1º semestre de 2026 cairia a −7,6% do MDIC.
- Sem item nem NCM na tabela: capitulo_ncm e uf_despacho saem vazios.

O CSV marca a origem de cada linha na coluna `fonte` (DI | DUIMP).

Uso:
  spark3-submit --master yarn --deploy-mode client \
    --jars /gfis2/jars/ojdbc8.jar \
    /gfis2/pipeline/gap_tributario/snapshot_siscomex.py \
    --saida /gfis2/pipeline/gap_tributario/siscomex_importacoes.csv \
    [--database-bronze gfis2_dev] [--database-prata gfis2_dev]
"""

import argparse
import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JDBC_URL = "jdbc:oracle:thin:@10.1.1.132:1521/cent"
# A DUIMP só é legível pelo sinônimo no ARMA; o mesmo usuário vale nos dois.
JDBC_URL_DUIMP = "jdbc:oracle:thin:@10.1.1.132:1521/arma"
USER = os.environ.get("SISCOMEX_ORACLE_USER", "WEB_GFIS2")
PASSWORD = os.environ.get("SISCOMEX_ORACLE_PASSWORD")
if not PASSWORD:
    raise SystemExit(
        "Defina SISCOMEX_ORACLE_PASSWORD no ambiente antes de rodar.\n"
        "  export SISCOMEX_ORACLE_PASSWORD='...'"
    )
DRIVER = "oracle.jdbc.driver.OracleDriver"

TABELA_CADASTRO = "gfis2_bronze.b_contribuinte"

# Bronze da DI: um item por linha, de TODAS as versões. O left join mantém as
# versões sem item, que contam na escolha da última versão (como no MAX(SEQ)
# sobre o cabeçalho inteiro que esta query fazia no Oracle) e depois caem por
# não ter valor.
_SQL_DI = """
    SELECT d.TDS_NUM_DECL           AS num_decl,
           d.TDS_SEQ_DECL           AS seq_decl,
           d.TDS_DATA_DESEMBARACO   AS data_desembaraco,
           d.TDS_UF_DESPACHO        AS uf_despacho,
           d.TDS_UF_IMPORTADOR      AS uf_importador,
           d.TDS_CNPJ               AS cnpj,
           d.TDS_TIPO_DECL          AS tipo_decl,
           d.TDS_SITUACAO           AS situacao,
           CASE WHEN i.TDI_TDS_NUM_DECL IS NULL THEN 0 ELSE 1 END AS tem_item,
           i.TDI_TNM_COD_NCM        AS ncm,
           i.TDI_VALOR_BASE_CALC_II AS cif
      FROM APL_SISCOMEX.TAB_DECL_SISCOMEX d
      LEFT JOIN APL_SISCOMEX.TAB_ITEM_SISCOMEX i
        ON d.TDS_NUM_DECL = i.TDI_TDS_NUM_DECL AND d.TDS_SEQ_DECL = i.TDI_TDS_SEQ_DECL
"""


# Bronze da DUIMP: todas as versões. Vigência, registro e versão escolhida
# são filtrados no Spark.
_SQL_DUIMP = """
    SELECT NUMERODUIMP                              AS num_decl,
           VERSAODECLARACAO                         AS versao,
           IDDUIMP                                  AS id_duimp,
           STVIGENTE                                AS vigente,
           DATAHORAREGISTRO                         AS registro,
           IDUFIMPORTADOR                           AS id_uf,
           CNPJIMPORTADOR                           AS cnpj,
           VLMERCADORIALOCALDESCARGAREAL            AS cif
      FROM DUIMP
"""

# Bronze da CARGA: a chegada de cada carga, com o IDDUIMP da versão.
_SQL_CARGA = """
    SELECT IDDUIMP     AS id_duimp,
           DATACHEGADA AS chegada
      FROM APL_PUCOMEX.CARGA@CENTRAL
"""

# IDUFIMPORTADOR → sigla: índice da UF em ordem alfabética do nome.
_UF_POR_ID_DUIMP = dict(
    enumerate(
        [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
            "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
        ],
        start=1,
    )
)

COLUNAS_SAIDA = [
    "ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_importador", "dis", "cif_brl", "fonte",
]


def _ler_oracle(spark, url, sql):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("query", sql)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", DRIVER)
        .option("fetchsize", "5000")
        .load()
    )


def _gravar_tabela(spark, df, tabela):
    """Sobrescreve `tabela` (Iceberg) com `df`, criando-a na primeira vez.

    Temp view + INSERT OVERWRITE, o idioma dos jobs da gold da casa.
    """
    view = "tmp_" + tabela.replace(".", "_")
    df.createOrReplaceTempView(view)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tabela} USING iceberg AS SELECT * FROM {view} WHERE 1 = 0"
    )
    spark.sql(f"INSERT OVERWRITE TABLE {tabela} SELECT * FROM {view}")
    print(f"[GRAVADO] {tabela}")


def _norm_cnpj(coluna):
    """CNPJ como string de 14 dígitos, sem '.0' de decimal e com zeros à esquerda."""
    limpo = F.regexp_replace(coluna.cast("string"), r"\.0+$", "")
    return F.lpad(limpo, 14, "0")


def main():
    parser = argparse.ArgumentParser(description="Snapshot agregado de importações Siscomex")
    parser.add_argument("--saida", required=True, help="Caminho do CSV de saída")
    parser.add_argument("--database-bronze", default="gfis2_dev",
                        help="database das cópias de origem (default: gfis2_dev)")
    parser.add_argument("--database-prata", default="gfis2_dev",
                        help="database do agregado (default: gfis2_dev)")
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

    # --- Bronze: cópia das origens, todas as versões --------------------------
    # Cada origem é lida do Oracle uma vez; as regras rodam sobre a cópia
    # gravada, o mesmo caminho de um reprocessamento.
    for sql, url, nome in (
        (_SQL_DI, JDBC_URL, "b_siscomex_di"),
        (_SQL_DUIMP, JDBC_URL_DUIMP, "b_duimp"),
        (_SQL_CARGA, JDBC_URL_DUIMP, "b_duimp_carga"),
    ):
        _gravar_tabela(spark, _ler_oracle(spark, url, sql), f"{args.database_bronze}.{nome}")
    bronze_di = spark.table(f"{args.database_bronze}.b_siscomex_di")
    bronze_duimp = spark.table(f"{args.database_bronze}.b_duimp")
    bronze_carga = spark.table(f"{args.database_bronze}.b_duimp_carga")

    # --- DI: última versão, desembaraçada, tipo 01 ou nulo --------------------
    # A última versão é escolhida entre TODOS os cabeçalhos, com ou sem item e
    # com ou sem data; só depois caem os sem data e os de outro tipo.
    ultima = bronze_di.groupBy("num_decl").agg(F.max("seq_decl").alias("seq_decl"))
    itens = (
        bronze_di.join(ultima, on=["num_decl", "seq_decl"], how="inner")
        .filter(F.col("tem_item") == 1)
        .filter(F.col("data_desembaraco").isNotNull())
        .filter(F.col("tipo_decl").isNull() | (F.col("tipo_decl") == "01"))
        .withColumn("ano", F.year("data_desembaraco"))
        .withColumn("trimestre", F.quarter("data_desembaraco"))
        .withColumn("capitulo_ncm", F.floor(F.col("ncm") / 1000000))
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
    agregado_di = (
        resolvido.groupBy("ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_final")
        .agg(
            F.countDistinct("num_decl").alias("dis"),
            F.sum("cif").alias("cif_brl"),
        )
        .withColumn("fonte", F.lit("DI"))
    )

    # --- DUIMP ---------------------------------------------------------------
    duimp = bronze_duimp.filter(
        (F.col("vigente") == "S") & F.col("registro").isNotNull()
    )
    versao_vigente = Window.partitionBy("num_decl").orderBy(
        F.desc("versao"), F.desc("id_duimp")
    )
    duimp = (
        duimp.withColumn("rk", F.row_number().over(versao_vigente))
        .filter(F.col("rk") == 1)
        .drop("rk")
    )
    # greatest do Spark ignora nulos: sem chegada, vale o registro.
    # O diagnóstico achou uma carga por DUIMP; o MAX só impede que uma
    # duplicata futura multiplique o valor no join.
    carga = (
        bronze_carga.filter(F.col("id_duimp").isNotNull())
        .groupBy("id_duimp")
        .agg(F.max("chegada").alias("chegada"))
    )
    duimp = (
        duimp.join(carga, on="id_duimp", how="left")
        .withColumn(
            "referencia",
            F.greatest(
                F.col("registro").cast("timestamp"), F.col("chegada").cast("timestamp")
            ),
        )
        .withColumn("ano", F.year("referencia"))
        .withColumn("trimestre", F.quarter("referencia"))
        .cache()
    )

    print("\n=== DUIMP MA: TRIMESTRE DO REGISTRO × TRIMESTRE ADOTADO (R$ mi) ===")
    (
        duimp.filter(F.col("id_uf").cast("int") == 10)
        .withColumn("tri_registro", F.concat_ws(
            "-T", F.year("registro").cast("string"), F.quarter("registro").cast("string")
        ))
        .withColumn("tri_adotado", F.concat_ws("-T", F.col("ano").cast("string"), F.col("trimestre").cast("string")))
        .groupBy("tri_registro", "tri_adotado")
        .agg(
            F.count("*").alias("duimps"),
            F.sum(F.col("chegada").isNotNull().cast("int")).alias("com_chegada"),
            F.round(F.sum("cif") / 1e6, 1).alias("cif_mi"),
        )
        .orderBy("tri_registro", "tri_adotado")
        .show(60, truncate=False)
    )

    mapa_uf = F.create_map(
        *[x for i, uf in _UF_POR_ID_DUIMP.items() for x in (F.lit(i), F.lit(uf))]
    )
    duimp_resolvido = (
        duimp.withColumn("cnpj_norm", _norm_cnpj(F.col("cnpj")))
        .join(cadastro, on="cnpj_norm", how="left")
        .withColumn(
            "uf_final",
            F.coalesce(
                mapa_uf[F.col("id_uf").cast("int")],
                F.nullif(F.trim(F.col("uf_cadastro")), F.lit("")),
                F.lit("NI"),
            ),
        )
    )
    agregado_duimp = (
        duimp_resolvido.groupBy("ano", "trimestre", "uf_final")
        .agg(
            F.countDistinct("num_decl").alias("dis"),
            F.sum("cif").alias("cif_brl"),
        )
        .withColumn("capitulo_ncm", F.lit(None).cast("int"))
        .withColumn("uf_despacho", F.lit(None).cast("string"))
        .withColumn("fonte", F.lit("DUIMP"))
    )

    colunas = ["ano", "trimestre", "capitulo_ncm", "uf_despacho", "uf_final", "dis", "cif_brl", "fonte"]
    agregado = (
        agregado_di.select(*colunas)
        .unionByName(agregado_duimp.select(*colunas))
        .orderBy("ano", "trimestre", "fonte", "capitulo_ncm", "uf_despacho", "uf_final")
    )

    print("\n=== TOTAL POR ANO E FONTE, IMPORTADOR MA ===")
    (
        agregado.filter(F.col("uf_final") == "MA")
        .groupBy("ano", "fonte")
        .agg(F.sum("dis").alias("dis"), (F.sum("cif_brl") / 1e9).alias("cif_bi"))
        .orderBy("ano", "fonte")
        .show(60, truncate=False)
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

    # --- Prata: o mesmo agregado do CSV --------------------------------------
    prata = agregado.select(
        F.col("ano").cast("int").alias("ano"),
        F.col("trimestre").cast("int").alias("trimestre"),
        F.col("capitulo_ncm").cast("int").alias("capitulo_ncm"),
        F.col("uf_despacho").cast("string").alias("uf_despacho"),
        F.col("uf_final").cast("string").alias("uf_importador"),
        F.col("dis").cast("bigint").alias("qtd_declaracoes"),
        F.col("cif_brl").cast("decimal(18,2)").alias("cif_brl"),
        F.col("fonte").cast("string").alias("fonte"),
        F.current_date().alias("dt_snapshot"),
    )
    _gravar_tabela(spark, prata, f"{args.database_prata}.s_gap_importacoes")

    linhas = agregado.collect()
    with open(args.saida, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(COLUNAS_SAIDA)
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
                    r["fonte"],
                ]
            )

    print(f"\nSnapshot escrito em {args.saida} ({len(linhas)} linhas agregadas)")
    spark.stop()


if __name__ == "__main__":
    main()
