import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
import json
import sys

def get_download_links(url, auth=None):
    """Extrai os links de download de arquivos CSV da página do MDIC."""
    headers = {}
    if auth and auth.get("username") and auth.get("password"):
        print("Autenticação configurada, mas não utilizada para este site específico do MDIC.")

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    
    links = []
    # Exportação por NCM
    export_ncm_section = soup.find('p', string=lambda text: text and 'Exportação -' in text)
    if export_ncm_section:
        table = export_ncm_section.find_next('table')
        if table:
            for a_tag in table.find_all('a', href=True):
                year = a_tag.get_text(strip=True)
                if year.isdigit():
                    links.append({'type': 'exportacao', 'category': 'ncm', 'year': year, 'url': a_tag['href']})

    # Importação por NCM
    import_ncm_section = soup.find('p', string=lambda text: text and 'Importação -' in text)
    if import_ncm_section:
        table = import_ncm_section.find_next('table')
        if table:
            for a_tag in table.find_all('a', href=True):
                year = a_tag.get_text(strip=True)
                if year.isdigit():
                    links.append({'type': 'importacao', 'category': 'ncm', 'year': year, 'url': a_tag['href']})
                    
    return links

def download_and_load_to_hdfs(spark, links, hdfs_base_path):
    """Baixa os arquivos e os carrega no HDFS."""
    for link_info in links:
        file_type = link_info['type']
        category = link_info['category']
        year = link_info['year']
        url = link_info['url']
        
        print(f"Processando {file_type} {category} para o ano {year} da URL: {url}")
        
        try:
            file_content = requests.get(url, stream=True)
            file_content.raise_for_status()
            
            # HDFS
            hdfs_path = f"{hdfs_base_path}/{file_type}/{category}/ano={year}/{file_type}_{category}_{year}.csv"
            
            local_temp_file = f"/tmp/{file_type}_{category}_{year}.csv"
            with open(local_temp_file, 'wb') as f:
                for chunk in file_content.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            df = spark.read.csv(local_temp_file, sep=';', header=True, inferSchema=True)
            df = df.withColumn("tipo_dado", lit(file_type))
            df = df.withColumn("categoria_dado", lit(category))
            df.write.mode('overwrite').csv(hdfs_path, header=True, sep=';')
            print(f"Arquivo {file_type}_{category}_{year}.csv carregado com sucesso em {hdfs_path}")
            os.remove(local_temp_file)
            
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar {url}: {e}")
        except Exception as e:
            print(f"Erro ao processar {file_type} {category} para o ano {year}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: spark-submit mdic_comex_crawler.py <caminho_para_config.json>")
        sys.exit(1)

    config_file_path = sys.argv[1]
    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração não encontrado em {config_file_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Erro: Arquivo de configuração JSON inválido em {config_file_path}")
        sys.exit(1)

    MDIC_URL = config.get("mdic_url")
    HDFS_BASE_PATH = config.get("hdfs_base_path")
    AUTH_CONFIG = config.get("auth")

    if not MDIC_URL or not HDFS_BASE_PATH:
        print("Erro: mdic_url ou hdfs_base_path não encontrados no arquivo de configuração.")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("MDICComexCrawler") \
        .getOrCreate()

    print("MDIC...")
    download_links = get_download_links(MDIC_URL, AUTH_CONFIG)
    print(f"Encontrados {len(download_links)} links de download.")
    
    if download_links:
        download_and_load_to_hdfs(spark, download_links, HDFS_BASE_PATH)
    else:
        print("Nenhum link de download encontrado. Verifique a URL ou a estrutura da página.")

    spark.stop()
    print("MDIC concluído.")
