#!/usr/bin/env bash
# Roda um job do Gap Tributário no cluster, com Iceberg e o driver Oracle.
#
#   ./gap_tributario/run_gap_job.sh <job.py> [argumentos do job]
#
# Exemplos:
#   ./gap_tributario/run_gap_job.sh snapshot_siscomex.py --saida .../siscomex_importacoes.csv
#   ./gap_tributario/run_gap_job.sh carga_gold_gap.py carregar --csv-dir /tmp/gap_tributario/ouro
#   ./gap_tributario/run_gap_job.sh carga_gold_gap.py promover
#   ./gap_tributario/run_gap_job.sh corrige_comments_gold.py --aplicar
#
# Executar a partir da raiz do pipeline; os .py ficam em gap_tributario/.
# Modelado em 3-gold/processamento_pyspark/run_g_arrecadacao.sh.

set -e
source ~/.bashrc
conda deactivate

if [ $# -eq 0 ]; then
  echo "uso: $0 <job.py> [argumentos]" >&2
  exit 2
fi
JOB="$1"
shift

spark3-submit --master yarn \
  --deploy-mode client \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.executor.memory=4g \
  --jars /gfis2/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar,/gfis2/jars/ojdbc8.jar \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  "gap_tributario/$JOB" "$@"
