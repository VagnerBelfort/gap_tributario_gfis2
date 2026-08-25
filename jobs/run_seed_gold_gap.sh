#!/usr/bin/env bash
# Cria e semeia as tabelas da camada Gold do Gap Tributário.
# Cria: g_gap_resultado, g_gap_decomposicao, g_gap_proveniencia
# Destino padrão: gfis2_dev. Também publicado em gfis2_ouro em 2026-07-29
# (dados de PROTÓTIPO — ver cabeçalho do .py; serão substituídos pelo
# pipeline real bronze→prata→ouro).
#
# Modelado em 3-gold/processamento_pyspark/run_g_arrecadacao.sh
# Executar a partir da raiz do repositório: ./gap_tributario/run_seed_gold_gap.sh
#
# Sem etapa de HDFS: os dados estão embutidos no .py, não há parquet de origem.

set -e
source ~/.bashrc
conda deactivate

# Sem argumentos → semeia gfis2_dev. Repassa qualquer flag ao job.
if [ $# -eq 0 ]; then
  set -- --database gfis2_dev
fi

spark3-submit --master yarn \
  --deploy-mode client \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.executor.memory=4g \
  --jars /gfis2/jars/iceberg-spark-runtime-3.3_2.12-1.4.3.jar \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  gap_tributario/seed_gold_mock.py "$@"
