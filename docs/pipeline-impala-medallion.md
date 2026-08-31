# Pipeline Gap Tributário → Impala (bronze / prata / ouro)

**Status:** design aprovado nas decisões estruturais, pendente de confirmação de infra.
**Data:** 2026-07-15
**Contexto:** a modelagem VRR foi aprovada. Este documento descreve como levá-la ao
Cloudera (PySpark 3 / `spark-submit3` / Airflow) e quais tabelas da ouro a tela consome.

---

## ✅ Verificado no cluster (2026-07-15, `Sefazbige01`)

As inferências foram checadas contra o ambiente real. Resultado:

| # | Inferência original | Veredito |
|---|---|---|
| 1 | Bancos `gfis2_bronze` / `gfis2_prata` / `gfis2_ouro` | ✅ **Confirmado** — `lista_tabelas_impala.txt` |
| 2 | Prefixos `b_` / `s_` / `g_` | ✅ **Confirmado** |
| 3 | `gfis2_dev` para desenvolvimento | ✅ **Confirmado** — já existe (`g_arrecadacao_spark_arrec`) |
| 4 | Tabelas em **Parquet** | ❌ **REFUTADO — são Iceberg.** `USING iceberg` + `TBLPROPERTIES ('transactional'='false')`. Exige `--jars /gfis2/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar` e o catálogo `SparkSessionCatalog` type=hive |
| 5 | Comando `spark-submit3` | ❌ **REFUTADO — é `spark3-submit`.** `spark-submit3` não existe na máquina |
| 6 | `TINYINT` para trimestre/ordem | ❌ **REFUTADO** — Iceberg não tem tipo de 1 byte; usar `INT`. Não há um só `TINYINT` no DDL do projeto |
| 7 | Precisa mover arquivos ao HDFS | ❌ **REFUTADO** — o `hdfs dfs -put` do `2-silver` move **parquets de dados** (`/gfis2/datasets` → `/user/gfis2/tmp/`). O job de gold roda `--deploy-mode client` lendo o `.py` do disco local. Sem parquet de origem, não há etapa de HDFS |
| 8 | Driver com Python 3.8+ | ❌ **REFUTADO — Python 3.6.8.** O run script faz `conda deactivate`, caindo no `/usr/bin/python3`. Ver risco 11 |
| 9 | Cluster sem egress | ⚠️ Não testado nesta sessão |
| 10 | Oracle Siscomex inalcançável | ⚠️ **Indício forte do contrário**: `/gfis2/jars/ojdbc8.jar` existe e há `/gfis2/pipeline/exploracao_siscomex/`. Ver risco 5 |

### Convenções reais da casa

```bash
# 3-gold/processamento_pyspark/run_g_arrecadacao.sh — o padrão a seguir
set -e
source ~/.bashrc
conda deactivate                      # → cai no Python 3.6.8 do sistema

spark3-submit --master yarn \
  --deploy-mode client \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.executor.memory=4g \
  --jars /gfis2/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  3-gold/processamento_pyspark/pyspark_processa_g_arrecadacao.py
```

```bash
# impala-shell — host real é o bige02, com ca_cert explícito
impala-shell -i sefazbige02.sefaz.ma.gov.br -d default -k --ssl \
  --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem
```

Ordem do DDL no Spark SQL: `USING` vem **antes** do `COMMENT` de tabela —
`CREATE TABLE (...) USING iceberg COMMENT '...' TBLPROPERTIES (...)`.

`INVALIDATE METADATA` recebe **tabela**, não database: `INVALIDATE METADATA
gfis2_dev.g_gap_resultado`, uma por vez.

As DAGs usam `SSHOperator` + `ExternalTaskSensor` contra outras DAGs
(`GFIS2_CAMADA_SILVER_PRD`), com as queries em `Variable.get(...)` do Airflow —
não `BashOperator` local, como eu havia desenhado no §8.

---

## 1. Decisões estruturais

| Decisão | Escolha | Razão |
|---|---|---|
| Onde a fórmula executa | `MotorVRR` no **driver**, via zip stdlib | O motor é aritmética escalar sobre 7 números; a ouro tem ~320 linhas. Spark não acelera nada e reimplementar duplicaria a fórmula que o `CLAUDE.md` proíbe mutar sem aprovação metodológica. |
| Ingestão externa | **Landing zone** (cluster fechado) | Sem egress, o Spark não alcança BCB/IBGE/MDIC. |
| Cascata de fontes | Prata guarda **todos os candidatos** | Auditabilidade: dá para provar por que um número mudou. |
| Fonte de ICMS na ouro | **`g_arrecadacao` (GFIS2)** manda; SIGDEF vira candidato | Evita publicar um ICMS que contradiz o número oficial do próprio cluster. |
| Atualização da ouro | **Rebuild completo, sem partição** | ~320 linhas. Idempotente; revisão de LDO se autocorrige. |
| Cadência | **Mensal + sensores na landing** | MDIC (mensal) é o insumo mais rápido que move o gap. |
| Publicação | **Write-Audit-Publish** | Dado velho e correto > dado novo e errado. |
| Formato | Parquet + Snappy | Padrão do cluster; sem introduzir Kudu por 320 linhas. |

---

## 2. Arquitetura

```
┌─ EDGE NODE (Python 3.8+ / polars — FORA do Spark) ────────────────┐
│  Reusa código JÁ TESTADO do repo:                                 │
│    SigdefParser        .xls  → parquet   (fix offset coluna 21)   │
│    imesc_pib           .pdf  → parquet                            │
│    amf_renuncia        .xlsx → parquet                            │
│    httpx / sidrapy     API   → json      (requer proxy/egress)    │
│  Deposita em: hdfs:///landing/gap/{fonte}/{ano}/                  │
└───────────────────────────┬───────────────────────────────────────┘
                            ▼
┌─ BRONZE (spark-submit3) ──────────────────────────────────────────┐
│  Cópia fiel do bruto + metadados de ingestão.                     │
│  Sem regra de negócio. Nada de filtro, nada de conversão.         │
│    b_gap_mdic_comex · b_gap_ptax · b_gap_imesc_pib                │
│    b_gap_amf_renuncia · b_gap_sigdef                              │
│  gfis2_ouro.g_arrecadacao → NÃO É INGERIDA (já existe, read-only) │
└───────────────────────────┬───────────────────────────────────────┘
                            ▼
┌─ PRATA (spark-submit3) ───────────────────────────────────────────┐
│  s_gap_componente — fato LONGO, um candidato por linha.           │
│  Aqui moram as regras: filtro NCM, PTAX×FOB, mês→trimestre→ano.   │
│  TODOS os candidatos coexistem. Nada é eleito ainda.              │
└───────────────────────────┬───────────────────────────────────────┘
                            ▼
┌─ OURO (spark-submit3 --py-files gap_engine.zip) ──────────────────┐
│  Elege vencedor por config/fontes.yaml → collect() ~40 linhas     │
│  → MotorVRR.calcular() + decompor_gap() no driver                 │
│  → escreve em gfis2_stg (staging) → gate → swap para gfis2_ouro   │
│    g_gap_resultado · g_gap_decomposicao · g_gap_proveniencia      │
└───────────────────────────────────────────────────────────────────┘
```

**Por que o motor roda no driver e não distribuído:** `MotorVRR.calcular()` recebe
um `DadosVRR` com 7 `Decimal` e devolve 5. A ouro inteira (2019–2026, anual +
trimestral) tem ~320 linhas. O `collect()` de 40 linhas é seguro por construção.
O trabalho pesado — o parquet do GFIS2, os CSVs do MDIC — acontece antes, em SQL
distribuído.

---

## 3. As tabelas da ouro (o que a tela consome)

### 3.1 `gfis2_ouro.g_gap_resultado`

Grão: um período. Anual e trimestral na mesma tabela.
Serve: KPIs, evolução 2019–2026, waterfall VRR, gráfico trimestral, tabela de anos,
tabela trimestral. **Seis dos nove blocos da tela.**

```sql
CREATE TABLE IF NOT EXISTS gfis2_ouro.g_gap_resultado (
  ano              INT       COMMENT 'Ano de referência',
  tipo_periodo     STRING    COMMENT 'A = anual, T = trimestral',
  nro_trimestre    TINYINT   COMMENT '0 quando anual, 1-4 quando trimestral',

  -- componentes da fórmula (R$ milhões)
  vab              DECIMAL(18,2) COMMENT 'Valor Adicionado Bruto',
  exportacoes      DECIMAL(18,2) COMMENT 'Exportações FOB convertidas por PTAX',
  importacoes      DECIMAL(18,2) COMMENT 'Importações FOB convertidas por PTAX',
  base_calculo     DECIMAL(18,2) COMMENT 'VAB - Exportações + Importações',
  aliquota         DECIMAL(6,4)  COMMENT 'Alíquota modal (0.18 ou 0.20)',
  ptax_media       DECIMAL(10,4) COMMENT 'USD/BRL médio do período',

  -- resultados
  icms_potencial   DECIMAL(18,2) COMMENT 'base_calculo × aliquota',
  icms_arrecadado  DECIMAL(18,2),
  vrr              DECIMAL(10,6) COMMENT 'icms_arrecadado / icms_potencial',
  gap_absoluto     DECIMAL(18,2) COMMENT 'icms_potencial - icms_arrecadado',
  gap_percentual   DECIMAL(8,4)  COMMENT '(gap_absoluto / icms_potencial) × 100',

  -- qualificadores que a tela usa para badge/caveat
  flg_parcial      BOOLEAN COMMENT 'período incompleto (ex.: 2026 T1-T2)',
  flg_estimado     BOOLEAN COMMENT 'VAB estimado — IBGE tem lag de ~2 anos',

  -- proveniência resumida (detalhe em g_gap_proveniencia)
  fonte_vab        STRING COMMENT 'imesc | ibge_sidra | manual',
  fonte_icms       STRING COMMENT 'gfis2 | sigdef',
  fonte_imp        STRING COMMENT 'siscomex | mdic_ncm_filtrado | mdic_bruto',
  fonte_exp        STRING COMMENT 'mdic',

  -- rastreabilidade do run
  dt_calculo       TIMESTAMP,
  versao_engine    STRING COMMENT 'gap_tributario.__version__',
  id_execucao      STRING COMMENT 'Airflow run_id'
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Chave lógica: `(ano, tipo_periodo, nro_trimestre)`. Impala não impõe PK em Parquet —
a unicidade é garantida pelo rebuild completo.

### 3.2 `gfis2_ouro.g_gap_decomposicao`

Grão: ano × componente. Anual apenas — a renúncia da AMF Tabela 7 é anual por natureza.
Serve: bloco "Decomposição do Gap — Policy × Compliance" e as barras de modalidade.

```sql
CREATE TABLE IF NOT EXISTS gfis2_ouro.g_gap_decomposicao (
  ano                     INT,
  componente              STRING COMMENT 'GAP_TOTAL | POLICY | COMPLIANCE | MOD_CREDITO_PRESUMIDO | MOD_ISENCAO | MOD_REDUCAO_BASE',
  valor                   DECIMAL(18,2) COMMENT 'R$ milhões',
  pct_do_gap              DECIMAL(8,4)  COMMENT '% do gap total',
  vintage_ldo             STRING  COMMENT 'LDO-2022, LDO-2025, LDO-2026...',
  flg_compliance_negativo BOOLEAN COMMENT 'TRUE quando renúncia > gap total',
  dt_calculo              TIMESTAMP,
  id_execucao             STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Anos sem cobertura da AMF (2019–2021) simplesmente **não têm linha**. A tela já trata
isso: mostra a mensagem "Sem renúncia fiscal disponível para {ano} — a cobertura da
AMF Tabela 7 inicia em 2022".

### 3.3 `gfis2_ouro.g_gap_proveniencia`

Grão: período × variável. Serve: tabela "Componentes da fórmula & proveniência".
Espelha o dataclass `Proveniencia` (`models.py:150`) — mesmo contrato do PDF/Excel.

```sql
CREATE TABLE IF NOT EXISTS gfis2_ouro.g_gap_proveniencia (
  ano             INT,
  tipo_periodo    STRING,
  nro_trimestre   TINYINT,
  variavel        STRING  COMMENT 'VAB | ICMS Arrecadado | Exportações | Importações | PTAX média | Alíquota modal | Renúncia fiscal (ICMS)',
  valor           DECIMAL(18,4),
  origem          STRING  COMMENT 'sistema/instituição: IMESC, GFIS2, MDIC ComexStat, BCB...',
  fonte           STRING  COMMENT 'documento/tabela específica',
  dt_extracao     STRING  COMMENT 'ISO YYYY-MM-DD',
  observacoes     STRING  COMMENT 'caveats de cobertura/metodologia',
  fonte_vencedora STRING  COMMENT 'chave da fonte eleita pela cascata',
  ordem_cascata   TINYINT COMMENT 'posição da vencedora em fontes.yaml (1 = primeira opção)',
  dt_calculo      TIMESTAMP,
  id_execucao     STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

---

## 4. Queries da tela

Entregar isto para quem constrói o front-end. **A tela lê apenas `gfis2_ouro.g_gap_*`.**
Nunca prata, nunca bronze.

```sql
-- Bloco: seletor de período (popula o <select> de anos)
SELECT DISTINCT ano
FROM gfis2_ouro.g_gap_resultado
WHERE tipo_periodo = 'A'
ORDER BY ano;

-- Blocos: KPIs + waterfall VRR — período selecionado
-- (anual: tipo_periodo='A', nro_trimestre=0 | trimestral: 'T', 1-4)
SELECT vab, exportacoes, importacoes, base_calculo, aliquota, ptax_media,
       icms_potencial, icms_arrecadado, vrr, gap_absoluto, gap_percentual,
       flg_parcial, flg_estimado
FROM gfis2_ouro.g_gap_resultado
WHERE ano = ? AND tipo_periodo = ? AND nro_trimestre = ?;

-- Bloco: evolução 2019-2026 + tabela de anos consolidada
SELECT ano, aliquota, ptax_media, vab, exportacoes, importacoes,
       icms_potencial, icms_arrecadado, vrr, gap_absoluto, gap_percentual,
       flg_parcial, flg_estimado
FROM gfis2_ouro.g_gap_resultado
WHERE tipo_periodo = 'A'
ORDER BY ano;

-- Blocos: gráfico trimestral + tabela de detalhe trimestral
SELECT nro_trimestre, vab, exportacoes, importacoes,
       icms_potencial, icms_arrecadado, vrr, gap_absoluto
FROM gfis2_ouro.g_gap_resultado
WHERE ano = ? AND tipo_periodo = 'T'
ORDER BY nro_trimestre;

-- Bloco: decomposição policy × compliance (+ modalidades)
SELECT componente, valor, pct_do_gap, vintage_ldo, flg_compliance_negativo
FROM gfis2_ouro.g_gap_decomposicao
WHERE ano = ?
ORDER BY CASE componente
           WHEN 'GAP_TOTAL'  THEN 1
           WHEN 'POLICY'     THEN 2
           WHEN 'COMPLIANCE' THEN 3
           ELSE 4
         END;

-- Bloco: componentes da fórmula & proveniência
SELECT variavel, valor, origem, fonte, dt_extracao, observacoes
FROM gfis2_ouro.g_gap_proveniencia
WHERE ano = ? AND tipo_periodo = ? AND nro_trimestre = ?
ORDER BY variavel;
```

**Delta vs período anterior** (o "▲ +3,2% vs 2024" dos KPIs) **não é materializado** —
a tela calcula, como o protótipo já faz. Materializar criaria dependência cruzada
entre linhas sem ganho: T1 compara com T4 do ano anterior, e isso é uma regra de
apresentação, não de negócio.

**Formatação também é da tela.** A ouro guarda número; `R$ 21.065 mi` e `0,518` são
decisões de UI. O protótipo já tem `fmt()`, `fPct()` e `fVrr()`.

---

## 5. Prata — o fato de candidatos

```sql
CREATE TABLE IF NOT EXISTS gfis2_prata.s_gap_componente (
  ano           INT,
  tipo_periodo  STRING,
  nro_trimestre TINYINT,
  variavel      STRING  COMMENT 'vab | icms_arrecadado | exportacoes | importacoes | ptax | renuncia',
  fonte         STRING  COMMENT 'imesc | ibge_sidra | gfis2 | sigdef | siscomex | mdic_ncm_filtrado | mdic_bruto | amf',
  valor         DECIMAL(18,4),
  unidade       STRING  COMMENT 'BRL_MILHOES | USD_MILHOES | RATIO',
  flg_ok        BOOLEAN COMMENT 'FALSE = fonte falhou/indisponível no run',
  msg_erro      STRING  COMMENT 'motivo da falha, quando flg_ok = FALSE',
  observacoes   STRING,
  dt_extracao   STRING,
  dt_processo   TIMESTAMP,
  id_execucao   STRING
)
STORED AS PARQUET;
```

O ponto desta tabela é que **os perdedores sobrevivem**. Exemplo real, 2022:

| variavel | fonte | valor | resultado |
|---|---|---|---|
| `importacoes` | `siscomex` | NULL | indisponível (`flg_ok=FALSE`) |
| `importacoes` | `mdic_ncm_filtrado` | 21.924 | **vence** |
| `importacoes` | `mdic_bruto` | 38.786 | preservado — trânsito Itaqui |
| `icms_arrecadado` | `gfis2` | 10.917 | **vence** |
| `icms_arrecadado` | `sigdef` | 11.494 | preservado — divergência +5,3% |

É isto que transforma "confia em mim" em "olha a evidência". A diferença de
R$ 16.862 milhões entre `mdic_bruto` e `mdic_ncm_filtrado` é a prova material de que
o filtro NCM faz o que promete.

---

## 6. Bronze

Cópia fiel do bruto. Sem regra de negócio — se um valor precisa ser convertido,
filtrado ou agregado, isso é prata. Toda tabela carrega `dt_ingestao`,
`arquivo_origem` e `id_execucao`.

```sql
CREATE TABLE IF NOT EXISTS gfis2_bronze.b_gap_mdic_comex (
  fluxo          STRING COMMENT 'EXP | IMP',
  co_ano         INT,
  co_mes         INT,
  co_ncm         STRING COMMENT 'NCM 8 dígitos — capítulo = 2 primeiros',
  sg_uf_ncm      STRING COMMENT 'ATENÇÃO: NÃO é domicílio fiscal',
  vl_fob         DECIMAL(20,2) COMMENT 'USD',
  dt_ingestao    TIMESTAMP,
  arquivo_origem STRING,
  id_execucao    STRING
)
PARTITIONED BY (ano_particao INT)
STORED AS PARQUET;
```

Bronze e prata **são particionadas por ano** (o CSV do MDIC e o parquet do GFIS2 são
grandes); a **ouro não é** (~320 linhas — particionar geraria small files sem ganho).
A assimetria é proposital.

Demais tabelas seguem o mesmo padrão: `b_gap_ptax`, `b_gap_imesc_pib`,
`b_gap_amf_renuncia`, `b_gap_sigdef`.

**`gfis2_ouro.g_arrecadacao` não é ingerida.** Já existe no Impala, é mantida por
outro time, e este pipeline apenas a lê. Nunca escreve nela.

---

## 7. Jobs PySpark

### 7.1 `jobs/bronze_ingest.py`

```python
"""Bronze — cópia fiel da landing zone. Zero regra de negócio."""
import argparse
from pyspark.sql import SparkSession, functions as F

def main(ano: str, run_id: str) -> None:
    spark = SparkSession.builder.appName(f"gap_bronze_{ano}").enableHiveSupport().getOrCreate()

    for fluxo in ("EXP", "IMP"):
        (spark.read
            .option("sep", ";").option("encoding", "latin1").option("header", True)
            .csv(f"hdfs:///landing/gap/mdic/{fluxo}_{ano}.csv")
            .select(
                F.lit(fluxo).alias("fluxo"),
                F.col("CO_ANO").cast("int").alias("co_ano"),
                F.col("CO_MES").cast("int").alias("co_mes"),
                F.col("CO_NCM").alias("co_ncm"),
                F.col("SG_UF_NCM").alias("sg_uf_ncm"),
                F.col("VL_FOB").cast("decimal(20,2)").alias("vl_fob"),
                F.current_timestamp().alias("dt_ingestao"),
                F.input_file_name().alias("arquivo_origem"),
                F.lit(run_id).alias("id_execucao"),
                F.lit(int(ano)).alias("ano_particao"),
            )
            .write.mode("overwrite").insertInto("gfis2_bronze.b_gap_mdic_comex"))

    # parquets já limpos, produzidos pelos parsers testados no edge node
    for fonte in ("imesc_pib", "amf_renuncia", "sigdef", "ptax"):
        (spark.read.parquet(f"hdfs:///landing/gap/{fonte}/{ano}/")
            .withColumn("dt_ingestao", F.current_timestamp())
            .withColumn("id_execucao", F.lit(run_id))
            .withColumn("ano_particao", F.lit(int(ano)))
            .write.mode("overwrite").insertInto(f"gfis2_bronze.b_gap_{fonte}"))

    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--ano", required=True)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()
    main(a.ano, a.run_id)
```

```bash
spark-submit3 \
  --master yarn --deploy-mode cluster \
  --name gap_bronze \
  --num-executors 4 --executor-memory 4G --executor-cores 2 \
  jobs/bronze_ingest.py --ano 2025 --run-id "$AIRFLOW_RUN_ID"
```

### 7.2 `jobs/silver_componentes.py`

Onde vivem as regras. Porta do `comex.py` para PySpark:

```python
"""Prata — produz TODOS os candidatos. Nenhum é eleito aqui."""
from pyspark.sql import SparkSession, functions as F

CAPITULOS_TRANSITO = [27, 31]  # config/ncm_transito.yaml — Itaqui
MESES_TRI = "CASE WHEN co_mes BETWEEN 1 AND 3 THEN 1 WHEN co_mes BETWEEN 4 AND 6 THEN 2 " \
            "WHEN co_mes BETWEEN 7 AND 9 THEN 3 ELSE 4 END"

def importacoes_candidatas(spark, ano, run_id):
    """Três candidatos para importação — a evidência do filtro NCM fica visível."""
    base = (spark.table("gfis2_bronze.b_gap_mdic_comex")
              .filter((F.col("ano_particao") == ano) & (F.col("fluxo") == "IMP")
                      & (F.col("sg_uf_ncm") == "MA"))
              .withColumn("capitulo", F.substring("co_ncm", 1, 2).cast("int"))
              .withColumn("nro_trimestre", F.expr(MESES_TRI)))

    ptax = (spark.table("gfis2_bronze.b_gap_ptax")
              .filter(F.col("ano_particao") == ano)
              .groupBy("nro_trimestre").agg(F.avg("cotacao_venda").alias("ptax")))

    def agregar(df, fonte):
        return (df.groupBy("nro_trimestre").agg(F.sum("vl_fob").alias("fob_usd"))
                  .join(ptax, "nro_trimestre")
                  .select(
                      F.lit(ano).alias("ano"), F.lit("T").alias("tipo_periodo"),
                      "nro_trimestre",
                      F.lit("importacoes").alias("variavel"), F.lit(fonte).alias("fonte"),
                      (F.col("fob_usd") * F.col("ptax") / 1_000_000)
                        .cast("decimal(18,4)").alias("valor"),
                      F.lit("BRL_MILHOES").alias("unidade"), F.lit(True).alias("flg_ok"),
                      F.lit(run_id).alias("id_execucao")))

    bruto = agregar(base, "mdic_bruto")
    filtrado = agregar(base.filter(~F.col("capitulo").isin(CAPITULOS_TRANSITO)),
                       "mdic_ncm_filtrado")
    # siscomex: quando o Oracle abrir, entra aqui como candidato de ordem 1
    return bruto.unionByName(filtrado)
```

O ICMS lê a ouro existente, sem tocá-la:

```python
def icms_candidatos(spark, ano, run_id):
    gfis2 = (spark.table("gfis2_ouro.g_arrecadacao")          # READ-ONLY
               .filter(F.col("per_aaaa") == ano)
               .groupBy(F.col("per_nro_trimestre").alias("nro_trimestre"))
               .agg(((F.sum("val_icms_normal") + F.sum("val_icms_imp")
                      + F.sum("val_icms_st_sda")) / 1_000_000).alias("valor"))
               .withColumn("fonte", F.lit("gfis2")))
    sigdef = (spark.table("gfis2_bronze.b_gap_sigdef")
                .filter((F.col("ano_particao") == ano) & (F.col("uf") == "MA"))
                .groupBy(F.expr(MESES_TRI).alias("nro_trimestre"))
                .agg((F.sum("icms_total") / 1_000_000).alias("valor"))
                .withColumn("fonte", F.lit("sigdef")))
    return gfis2.unionByName(sigdef)  # ambos preservados; a ouro elege
```

```bash
spark-submit3 --master yarn --deploy-mode cluster --name gap_silver \
  --num-executors 4 --executor-memory 4G \
  jobs/silver_componentes.py --ano 2025 --run-id "$AIRFLOW_RUN_ID"
```

### 7.3 `jobs/gold_calcula.py` — o único que importa o engine

```python
"""Ouro — elege a fonte, chama o MotorVRR aprovado, escreve na STAGING."""
from decimal import Decimal
import yaml
from pyspark.sql import SparkSession, functions as F

from gap_tributario.engine.vrr import MotorVRR            # ← do gap_engine.zip
from gap_tributario.engine.gap_decomposition import decompor_gap
from gap_tributario.models import DadosVRR, PeriodoCalculo, RenunciaFiscal

def eleger(candidatos, variavel, ordem):
    """Aplica a cascata de config/fontes.yaml: primeira fonte OK vence."""
    disponiveis = {c["fonte"]: c for c in candidatos
                   if c["variavel"] == variavel and c["flg_ok"]}
    for pos, fonte in enumerate(ordem, start=1):
        if fonte in disponiveis:
            return disponiveis[fonte], fonte, pos
    raise ValueError(f"Nenhuma fonte disponível para '{variavel}' — cascata: {ordem}")

def main(ano, run_id, perfil="cloudera"):
    spark = SparkSession.builder.appName(f"gap_gold_{ano}").enableHiveSupport().getOrCreate()

    # NOTA: no perfil cloudera a cascata de icms_arrecadado é [gfis2, sigdef] —
    # invertida em relação ao CLI local, para não contradizer a g_arrecadacao.
    with open("fontes.yaml") as f:
        cascatas = yaml.safe_load(f)["perfis"][perfil]

    # ~40 linhas: collect() é seguro por construção
    candidatos = [r.asDict() for r in
                  spark.table("gfis2_prata.s_gap_componente").collect()]

    linhas, provs, decomps = [], [], []
    for periodo in periodos_do(candidatos):          # (ano, 'A', 0), (ano, 'T', 1..4)
        cands = [c for c in candidatos if mesmo_periodo(c, periodo)]

        vab,  f_vab, o_vab = eleger(cands, "vab", cascatas["vab"])
        icms, f_icms, o_icms = eleger(cands, "icms_arrecadado", cascatas["icms_arrecadado"])
        exp,  f_exp, o_exp = eleger(cands, "exportacoes", cascatas["exportacoes"])
        imp,  f_imp, o_imp = eleger(cands, "importacoes", cascatas["importacoes"])
        ptax, _, _ = eleger(cands, "ptax", cascatas["ptax"])

        # ↓ a fórmula aprovada, intocada, exatamente como nos golden tests
        resultado = MotorVRR().calcular(DadosVRR(
            periodo=PeriodoCalculo(ano=periodo.ano, trimestre=periodo.trimestre),
            icms_arrecadado=Decimal(str(icms["valor"])),
            vab=Decimal(str(vab["valor"])),
            exportacoes_brl=Decimal(str(exp["valor"])),
            importacoes_brl=Decimal(str(imp["valor"])),
            aliquota_padrao=aliquota_de(periodo.ano),   # config/aliquotas.yaml
            ptax_media=Decimal(str(ptax["valor"])),
        ))
        linhas.append(para_linha(resultado, f_vab, f_icms, f_imp, f_exp, run_id))
        provs.extend(para_proveniencia(resultado, cands, run_id))

        if periodo.is_anual:
            renuncia = renuncia_do_ano(cands)   # AMF Tabela 7; None se sem cobertura
            if renuncia:
                decomps.extend(para_decomposicao(
                    decompor_gap(resultado, renuncia), run_id))

    # STAGING — a ouro só é tocada depois do gate (Write-Audit-Publish)
    escrever(spark, linhas,  "gfis2_stg.g_gap_resultado")
    escrever(spark, decomps, "gfis2_stg.g_gap_decomposicao")
    escrever(spark, provs,   "gfis2_stg.g_gap_proveniencia")
    spark.stop()
```

```bash
# gap_engine.zip: stdlib puro (models + engine), sem uma única dependência externa
cd src && zip -r ../gap_engine.zip gap_tributario/__init__.py \
                                   gap_tributario/models.py \
                                   gap_tributario/engine/ && cd ..

spark-submit3 \
  --master yarn --deploy-mode cluster --name gap_gold \
  --py-files gap_engine.zip \
  --files config/fontes.yaml,config/aliquotas.yaml \
  --num-executors 2 --executor-memory 2G --driver-memory 4G \
  jobs/gold_calcula.py --ano 2025 --run-id "$AIRFLOW_RUN_ID" --perfil cloudera
```

### 7.4 `jobs/quality_gate.py`

Audita a **staging**. Falha fechada.

```python
CHECKS = [
    ("golden_2022_vrr",     "Golden: VRR 2022 = 0,518 ± 0,002",
     "SELECT vrr FROM gfis2_stg.g_gap_resultado WHERE ano=2022 AND tipo_periodo='A'",
     lambda v: abs(v - 0.518) <= 0.002),
    ("golden_2022_imp",     "Golden: importações 2022 ∈ [20.000, 23.000] (filtro NCM ativo)",
     "SELECT importacoes FROM gfis2_stg.g_gap_resultado WHERE ano=2022 AND tipo_periodo='A'",
     lambda v: 20_000 <= v <= 23_000),
    ("base_positiva",       "base_calculo > 0 em todo período",
     "SELECT COUNT(*) FROM gfis2_stg.g_gap_resultado WHERE base_calculo <= 0",
     lambda v: v == 0),
    ("vrr_plausivel",       "VRR ∈ [0,15; 0,95] — pega o 2019=0,213 suspeito",
     "SELECT COUNT(*) FROM gfis2_stg.g_gap_resultado WHERE vrr NOT BETWEEN 0.15 AND 0.95",
     lambda v: v == 0),
    ("soma_trimestres",     "Σ trimestres ≈ anual (tolerância 1%)",
     SQL_SOMA_TRI, lambda v: v <= 0.01),
]
# qualquer check falso → sys.exit(1) → DAG vermelha → gold PERMANECE INTACTA
```

> O check `vrr_plausivel` vai **acusar o ano de 2019** (VRR=0,213), que o `CLAUDE.md`
> já registra como suspeito. Decidir antes do go-live: excluir 2019 do escopo,
> ou afrouxar o limite inferior e registrar o caveat. **Do jeito que está, a primeira
> execução falha.**

---

## 8. DAG Airflow

```python
"""gap_tributario_icms_ma — mensal, Write-Audit-Publish."""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
import pendulum

ANO = "{{ data_interval_start.year }}"
RUN = "{{ run_id }}"

def submit(job, extra=""):
    return (f"spark-submit3 --master yarn --deploy-mode cluster "
            f"--name gap_{job} {extra} "
            f"/opt/gap_tributario/jobs/{job}.py --ano {ANO} --run-id '{RUN}'")

with DAG(
    dag_id="gap_tributario_icms_ma",
    schedule="@monthly",
    start_date=pendulum.datetime(2019, 1, 1, tz="America/Fortaleza"),
    catchup=False,                # backfill é trigger manual e consciente
    max_active_runs=1,            # rebuild completo — nunca concorrente
    default_args={"retries": 2, "email_on_failure": True},
    tags=["gfis2", "gap-tributario", "sefaz-ma"],
) as dag:

    # Cluster fechado: sem sensor, não há como saber se a landing recebeu o dado
    with TaskGroup("aguarda_landing") as landing:
        for fonte in ("mdic", "imesc", "amf", "ptax"):
            FileSensor(
                task_id=f"sensor_{fonte}",
                filepath=f"/landing/gap/{fonte}/{ANO}/_SUCCESS",
                poke_interval=600, timeout=6 * 3600, mode="reschedule",
            )

    bronze = BashOperator(task_id="bronze_ingest",
                          bash_command=submit("bronze_ingest",
                                              "--num-executors 4 --executor-memory 4G"))

    silver = BashOperator(task_id="silver_componentes",
                          bash_command=submit("silver_componentes",
                                              "--num-executors 4 --executor-memory 4G"))

    # WRITE — escreve na staging, não na ouro
    gold_stg = BashOperator(
        task_id="gold_calcula_staging",
        bash_command=submit("gold_calcula",
            "--py-files /opt/gap_tributario/gap_engine.zip "
            "--files /opt/gap_tributario/config/fontes.yaml,"
            "/opt/gap_tributario/config/aliquotas.yaml "
            "--driver-memory 4G") + " --perfil cloudera")

    # AUDIT — se falhar aqui, a ouro nunca é tocada
    gate = BashOperator(task_id="quality_gate",
                        bash_command=submit("quality_gate"))

    # PUBLISH — swap staging → ouro (~320 linhas, segundos)
    publish = BashOperator(task_id="publish_gold",
                           bash_command=submit("publish_gold"))

    # Impala não enxerga escrita de Spark sem REFRESH — gotcha clássico
    refresh = BashOperator(
        task_id="impala_refresh",
        bash_command=(
            "impala-shell -i $IMPALA_HOST -q \""
            "REFRESH gfis2_ouro.g_gap_resultado; "
            "REFRESH gfis2_ouro.g_gap_decomposicao; "
            "REFRESH gfis2_ouro.g_gap_proveniencia;\""))

    landing >> bronze >> silver >> gold_stg >> gate >> publish >> refresh
```

**`REFRESH` não é detalhe.** Quando o Spark escreve arquivos novos, o Impala continua
servindo o cache de metadados antigo — a tela mostraria dado velho sem nenhum erro
aparente, e o debug disso é doloroso. `REFRESH` (não `INVALIDATE METADATA`, que é bem
mais caro) resolve.

---

## 9. Rastreabilidade da fórmula

O que garante que a ouro publica exatamente a metodologia aprovada:

```
tests/test_engine/test_vrr.py          ← golden 2022, VRR 0,518 ± 0,002
        │  testa
        ▼
src/gap_tributario/engine/vrr.py       ← MotorVRR — fórmula sob trava metodológica
        │  empacotado em (sem alteração)
        ▼
gap_engine.zip                         ← stdlib puro, ~4 arquivos
        │  --py-files
        ▼
jobs/gold_calcula.py (driver)          ← chama MotorVRR().calcular()
        │
        ▼
gfis2_ouro.g_gap_resultado             ← o que a tela mostra
```

Nenhum elo reimplementa a fórmula. O `quality_gate` roda o mesmo golden **contra a
staging em produção** — o teste unitário e o gate de produção verificam a mesma
asserção sobre o mesmo código.

---

## 10. Pendências e riscos

| # | Item | Severidade | Ação |
|---|---|---|---|
| 1 | **ICMS 2022 = 10.278 no protótipo aprovado** não corresponde a nenhuma fonte conhecida (GFIS2=10.917, SIGDEF=11.494). Origem desconhecida. | **Alta** | Investigar antes do go-live. A tela vai mudar de 10.278 → 10.917. Se 10.278 tem um critério de agregação legítimo, a decisão de fonte precisa ser revista. |
| 2 | Divergência **SIGDEF × GFIS2 de ~5,3%** (11.494 × 10.917) sem reconciliação. | Alta | A prata torna visível. Abrir item de reconciliação com a área de arrecadação. |
| 3 | `quality_gate` **reprova 2019** (VRR=0,213). | Alta | Decidir: excluir 2019, ou afrouxar limite + caveat. **A primeira execução falha como está.** |
| 4 | Cascata está **hardcoded no `cli.py`**, não lida do `fontes.yaml`. O perfil `cloudera` inverte a ordem do ICMS. | Média | Implementar leitura por perfil — o `fontes.yaml` já se declara "ponto de extensão". |
| 5 | Oracle Siscomex: se alcançável do cluster, **mata a dívida do filtro NCM**. | Média | Testar conectividade. O `siscomex.py` já tem a query certa (`TDS_UF_IMPORTADOR='MA'`). |
| 6 | Filtro NCM exclui cap.27/31 **inteiros** — remove importação legítima de combustível do MA. | Média | Paliativo consciente. Some com o item 5. |
| 7 | Nomes dos bancos são **inferência**. | Baixa | Confirmar com a equipe de dados. |
| 8 | Quem opera a **landing zone**? Não há dono definido. | Média | Definir: script no edge node? Airflow com proxy? |
| 9 | `CLAUDE.md` descreve `engine/seasonality.py`, que **não existe** — o IMESC deu VAB trimestral nativo e tornou o rateio desnecessário. | Baixa | Atualizar o `CLAUDE.md`. |
| 10 | `flg_estimado` para 2024+ depende do fallback de VAB; a regra de marcação precisa ser explícita. | Baixa | Definir: `flg_estimado = (fonte_vab != 'imesc' AND ano >= 2024)`? |
| 11 | **O `gap_engine.zip` NÃO roda no driver como está.** `models.py` usa `@dataclass` (3.7+) e `from __future__ import annotations` (3.7+); o driver é **Python 3.6.8** após o `conda deactivate`. Isso bloqueia a decisão central do §1 (motor aprovado como fonte única). | **Alta** | Apontar `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` para `/miniconda3/bin/python3.8`, que já existe na máquina. **Preferir isso** a rebaixar o motor para 3.6 — o `vrr.py` está sob trava metodológica e mexer nele exige aprovação. Validar que o Spark 3.5.4 do Cloudera aceita o 3.8. |
| 12 | O §8 desenha a DAG com `BashOperator` local; a casa usa `SSHOperator` + `ExternalTaskSensor` + queries em `Variable.get(...)`. | Média | Reescrever o §8 no padrão de `3-gold/dag/dag_gold_arrecadacao.py` antes de subir a DAG. |

---

## 11. Ordem de implementação sugerida

1. **Confirmar as inferências** (§ inferências) — barato, destrava tudo.
2. **Resolver o 10.278** (risco 1) — pode invalidar a decisão de fonte de ICMS.
3. **Decidir 2019** (risco 3) — senão a DAG nasce vermelha.
4. Cascata por perfil no `fontes.yaml` (risco 4) — com TDD, conforme o `CLAUDE.md`.
5. DDL das tabelas (bronze → prata → ouro → staging).
6. Parsers no edge node (reuso do que existe) + landing zone.
7. Jobs PySpark: bronze → prata → ouro.
8. `quality_gate` + `publish_gold`.
9. DAG.
10. Entregar § 4 (queries) para quem constrói a tela — **pode ser paralelizado a
    partir do passo 5**, assim que o DDL existir e houver dado de exemplo.
```
