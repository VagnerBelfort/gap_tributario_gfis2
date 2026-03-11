# Documentação da Solução ETL para Dados de Comércio Exterior do MDIC

## 1. Visão Geral

Esta solução automatiza a extração de dados brutos de comércio exterior (importação e exportação) do site do Ministério do Desenvolvimento, Indústria, Comércio e Serviços (MDIC) do Brasil. Os dados são processados por um script PySpark e carregados no HDFS (Hadoop Distributed File System) em um ambiente Cloudera. A orquestração de todo o processo é realizada pelo Apache Airflow.

### Componentes da Solução:

*   **Crawler PySpark:** Responsável por navegar na página do MDIC, identificar os links de download dos arquivos CSV e carregar esses arquivos diretamente no HDFS.
*   **HDFS (Cloudera):** Armazenamento dos dados brutos (RAW) em formato CSV, organizados por tipo de operação (importação/exportação), categoria (NCM) e ano.
*   **Apache Airflow:** Orquestrador que agenda e executa o job PySpark, garantindo a automação e monitoramento do pipeline de dados.

## 2. Pré-requisitos

Para implementar e executar esta solução, os seguintes pré-requisitos são necessários:

*   **Ambiente Cloudera:** Um cluster Cloudera com HDFS e Spark configurados e acessíveis.
*   **Apache Airflow:** Uma instância do Apache Airflow em execução, com acesso ao cluster Cloudera (via `spark-submit` e HDFS).
*   **Python 3.x:** Instalado nos workers do Airflow e nos nós do cluster Spark.
*   **Bibliotecas Python:** `requests` e `beautifulsoup4` para o crawler, e `pyspark` para o processamento de dados.

## 3. Estrutura de Arquivos

A solução consiste em três arquivos principais:

*   `mdic_comex_crawler.py`: O script PySpark que realiza o web scraping e a carga no HDFS.
*   `mdic_comex_dag.py`: O DAG do Airflow que orquestra a execução do script PySpark.
*   `mdic_comex_config.json`: Arquivo de configuração JSON que armazena parâmetros como URLs e caminhos do HDFS.

## 4. Script PySpark (`mdic_comex_crawler.py`)

Este script Python utiliza as bibliotecas `requests` e `BeautifulSoup` para fazer o download dos arquivos CSV do site do MDIC e o `PySpark` para ler, adicionar metadados e escrever os dados no HDFS. Ele agora lê suas configurações de um arquivo JSON.

### Funcionalidades:

1.  **`get_download_links(url, auth=None)`:**
    *   Navega até a URL especificada do MDIC.
    *   Analisa o HTML da página para encontrar os links de download dos arquivos CSV de importação e exportação, focando na seção de dados detalhada por NCM. Ele extrai o ano, o tipo de operação (exportação/importação) e a URL de cada arquivo.
    *   Aceita um dicionário `auth` para credenciais, embora não seja necessário para o site do MDIC, está implementado para extensibilidade.
2.  **`download_and_load_to_hdfs(spark, links, hdfs_base_path)`:**
    *   Itera sobre a lista de links obtidos.
    *   Para cada link, baixa o arquivo CSV para um diretório temporário local.
    *   Utiliza o Spark para ler o arquivo CSV temporário, inferir o esquema e adicionar colunas de metadados (`tipo_dado`, `categoria_dado`).
    *   Escreve o DataFrame resultante no HDFS no caminho especificado (`hdfs_base_path/{tipo_dado}/{categoria_dado}/ano={ano}/`). Os arquivos são salvos em formato CSV com cabeçalho e delimitador ponto e vírgula, no modo `overwrite`.
    *   Remove o arquivo temporário local após a carga no HDFS.

### Uso do Arquivo de Configuração:

O script espera o caminho para um arquivo JSON de configuração como seu primeiro argumento de linha de comando. Este arquivo deve conter:

*   `mdic_url`: URL da página do MDIC onde os arquivos estão localizados.
*   `hdfs_base_path`: Caminho base no HDFS onde os dados brutos serão armazenados (ex: `/user/cloudera/mdic_comex_raw`).
*   `auth`: (Opcional) Um objeto JSON com `username` e `password` para autenticação, se necessário.

Exemplo de `mdic_comex_config.json`:
```json
{
    "mdic_url": "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta?...",
    "hdfs_base_path": "/user/cloudera/mdic_comex_raw",
    "auth": {
        "username": null,
        "password": null
    }
}
```

## 5. DAG do Apache Airflow (`mdic_comex_dag.py`)

Este DAG define o pipeline de orquestração para executar o script PySpark. Ele utiliza o `BashOperator` para invocar o `spark-submit`, que por sua vez executa o script PySpark no cluster Spark.

### Configurações do DAG:

*   `dag_id`: `mdic_comex_crawler_dag`
*   `start_date`: Data de início do DAG (ex: `datetime(2023, 1, 1)`).
*   `schedule_interval`: Frequência de execução (ex: `@daily` para execução diária).
*   `catchup`: `False` para evitar execuções retroativas.
*   `tags`: `["mdic", "comex", "spark", "hdfs", "crawler"]`

### Task Principal (`run_mdic_comex_crawler`):

*   **Tipo:** `BashOperator`
*   **Comando:** Executa o `spark-submit` com os seguintes parâmetros:
    *   `--master`: Define o gerenciador de cluster Spark (ex: `yarn`).
    *   `--deploy-mode`: Modo de deploy (ex: `cluster` ou `client`).
    *   `--executor-memory`: Memória alocada para cada executor Spark.
    *   `--driver-memory`: Memória alocada para o driver Spark.
    *   `--num-executors`: Número de executores Spark.
    *   `SPARK_APP_PATH`: Caminho para o script PySpark (`mdic_comex_crawler.py`).
    *   `SPARK_CONFIG_PATH`: **Novo parâmetro.** Caminho para o arquivo de configuração JSON (`mdic_comex_config.json`).

É crucial que tanto o script PySpark quanto o arquivo de configuração JSON estejam acessíveis no ambiente onde o `spark-submit` é executado (geralmente HDFS ou um sistema de arquivos compartilhado).

## 6. Instruções de Implantação

Para implantar esta solução, siga os passos abaixo:

### 6.1. Preparação do Ambiente

1.  **Instalar Dependências:** Certifique-se de que as bibliotecas `requests` e `beautifulsoup4` estejam instaladas no ambiente onde o script PySpark será executado (nos nós do cluster Spark, se o deploy for em `cluster` mode, ou no nó do driver se for em `client` mode). No ambiente de desenvolvimento, você pode instalá-las via `pip`:
    ```bash
    pip install requests beautifulsoup4
    ```
2.  **Acesso ao HDFS:** O usuário que executa o `spark-submit` deve ter permissões de escrita no `HDFS_BASE_PATH` especificado no arquivo de configuração JSON.

### 6.2. Implantação do Script PySpark e Arquivo de Configuração

1.  **Copiar para o HDFS:** Copie o arquivo `mdic_comex_crawler.py` e o `mdic_comex_config.json` para um diretório acessível no HDFS. Por exemplo:
    ```bash
    hdfs dfs -mkdir -p /user/cloudera/airflow_scripts
    hdfs dfs -put mdic_comex_crawler.py /user/cloudera/airflow_scripts/
    hdfs dfs -put mdic_comex_config.json /user/cloudera/airflow_scripts/
    ```
    *Certifique-se de que o `SPARK_APP_PATH` e `SPARK_CONFIG_PATH` no DAG do Airflow apontem para estes locais.* 

### 6.3. Implantação do DAG do Airflow

1.  **Copiar para o Diretório de DAGs:** Copie o arquivo `mdic_comex_dag.py` para o diretório de DAGs do seu ambiente Airflow. O Airflow irá detectá-lo automaticamente.
    ```bash
    # Exemplo: assumindo que o diretório de DAGs é ~/airflow/dags
    cp mdic_comex_dag.py ~/airflow/dags/
    ```
2.  **Habilitar o DAG:** Acesse a interface web do Airflow, localize o DAG `mdic_comex_crawler_dag` e habilite-o.
3.  **Monitorar:** Monitore a execução do DAG na interface do Airflow para garantir que o job PySpark esteja sendo executado com sucesso e que os dados estejam sendo carregados no HDFS.

## 7. Considerações Adicionais

*   **Schema Evolution:** O script PySpark utiliza `inferSchema=True`. Em um ambiente de produção, é recomendável definir explicitamente o schema para evitar problemas caso a estrutura dos arquivos CSV mude.
*   **Particionamento:** Os dados são particionados por `tipo_dado`, `categoria_dado` e `ano`. Isso é uma boa prática para otimizar consultas em ferramentas como Hive ou Impala que leem do HDFS.
*   **Formato de Armazenamento:** Atualmente, os dados são salvos como CSV no HDFS. Para otimizar o desempenho e o uso de armazenamento, considere converter os dados para formatos colunares como Parquet ou ORC após a ingestão inicial.
*   **Tratamento de Erros:** O script PySpark inclui blocos `try-except` básicos. Para um ambiente de produção, é aconselhável implementar um tratamento de erros mais robusto, incluindo logging detalhado e mecanismos de retry.
*   **Segurança:** Certifique-se de que as credenciais e permissões de acesso ao HDFS e ao cluster Spark estejam configuradas de forma segura. O arquivo `mdic_comex_config.json` pode conter informações sensíveis (como senhas, se usadas). Considere usar ferramentas de gerenciamento de segredos (ex: HashiCorp Vault, AWS Secrets Manager, Google Secret Manager) e injetar essas credenciais no ambiente de execução do Spark, em vez de armazená-las diretamente no JSON.

## 8. Referências

*   [Página de Estatísticas de Comércio Exterior do MDIC](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta)
*   [Documentação Apache Spark](https://spark.apache.org/docs/latest/)
*   [Documentação Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
*   [Documentação Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
*   [Documentação Requests](https://requests.readthedocs.io/en/latest/)
