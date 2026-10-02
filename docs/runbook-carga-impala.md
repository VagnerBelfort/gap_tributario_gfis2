# Runbook — carga do Gap Tributário no Impala

Publica as importações (DI + DUIMP) e o gap anual de 2020 a 2025 nas tabelas
que o painel da SEFAZ lê. Uma extração alimenta tudo: o CSV do CLI, a prata e o
cálculo. A carga passa primeiro pelo `gfis2_dev` e chega à produção **por cópia**
do que foi validado.

| Camada | Tabela | Conteúdo | Quem grava |
|---|---|---|---|
| bronze | `b_siscomex_di` | DI × item, todas as versões, colunas usadas + chaves + CNPJ | `snapshot_siscomex.py` |
| bronze | `b_duimp`, `b_duimp_carga` | DUIMP e chegada da carga, todas as versões | `snapshot_siscomex.py` |
| prata | `s_gap_importacoes` | o agregado do CSV, com `qtd_declaracoes` e `dt_snapshot` | `snapshot_siscomex.py` |
| ouro | `g_gap_resultado`, `g_gap_decomposicao`, `g_gap_proveniencia` | o cálculo do CLI | `carga_gold_gap.py` |

**A estrutura da ouro não muda.** O painel lê essas três tabelas. O job aborta
se colunas, tipos, ordem ou valores categóricos (`fonte_*`, `variavel`,
`componente`) divergirem da produção.

Convenção: `AAAAMMDD` é a data do dia da carga. Os passos 1 a 3 rodam no
servidor `Sefazbige01`, a partir de `/gfis2/pipeline`; os passos 4 e 5 no
laptop, na raiz do repositório.

## 0. Levar o código ao servidor

```bash
scp scripts/snapshot_siscomex.py scripts/export_arrecadacao_agregada.py \
    jobs/carga_gold_gap.py jobs/carga_gold_regras.py jobs/corrige_comments_gold.py \
    jobs/run_gap_job.sh \
    gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/
```

## 1. Extração (servidor)

```bash
export SISCOMEX_ORACLE_PASSWORD='...'
./gap_tributario/run_gap_job.sh snapshot_siscomex.py \
    --saida /gfis2/pipeline/gap_tributario/siscomex_importacoes.csv
```

Grava `gfis2_dev.b_siscomex_di`, `gfis2_dev.b_duimp`, `gfis2_dev.b_duimp_carga`,
`gfis2_dev.s_gap_importacoes` e o CSV.

## 2. Arrecadação, no mesmo dia (servidor)

A `g_arrecadacao` muda todo dia. A data desta exportação é a data de corte que
vai para `dt_extracao`.

```bash
./gap_tributario/run_gap_job.sh export_arrecadacao_agregada.py \
    --saida-hdfs /tmp/gap_tributario/g_arrecadacao_agregada_AAAAMMDD
hdfs dfs -get /tmp/gap_tributario/g_arrecadacao_agregada_AAAAMMDD \
    /gfis2/pipeline/gap_tributario/
```

## 3. Trazer para o laptop

```bash
mv bases/siscomex_importacoes.csv bases/siscomex_importacoes_<data anterior>.bak.csv
scp gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/siscomex_importacoes.csv bases/
scp -r gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/g_arrecadacao_agregada_AAAAMMDD bases/
```

Apontar `parquet_base_path` em `config/aliquotas.yaml` para
`./bases/g_arrecadacao_agregada_AAAAMMDD/`.

## 4. Calcular (laptop)

```bash
uv run pytest tests/          # inclui o golden Siscomex 2022 contra o CSV novo
uv run python -m gap_tributario --exportar-ouro output/ouro_AAAAMMDD --anos 2020-2025
```

O golden de importações de 2022 (R$ 39.704 mi ± R$ 1 mi) precisa passar com o
CSV novo. **Se quebrar, pare e investigue; não atualize o golden.** Qualquer
log `fora do corredor de controle` também interrompe a carga.

## 5. Carregar no dev (laptop → servidor)

```bash
scp output/ouro_AAAAMMDD/*.csv gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/ouro_AAAAMMDD/
# no servidor:
hdfs dfs -mkdir -p /tmp/gap_tributario/ouro_AAAAMMDD
hdfs dfs -put -f /gfis2/pipeline/gap_tributario/ouro_AAAAMMDD/*.csv /tmp/gap_tributario/ouro_AAAAMMDD/
./gap_tributario/run_gap_job.sh carga_gold_gap.py carregar --csv-dir /tmp/gap_tributario/ouro_AAAAMMDD
```

## 6. Checklist antes de promover

1. `pytest` verde no passo 4, golden de 2022 incluído.
2. Importações de 2020 a 2025 dentro do corredor do MDIC (+2% a +13%): nenhum
   aviso de corredor no log do passo 4.
3. O passo 5 imprime `6`, `24` e `42` linhas (resultado, decomposição,
   proveniência), as mesmas contagens da produção.
4. O passo 5 imprime `ok` em "Importações do CSV × soma MA de gfis2_dev.s_gap_importacoes".
5. O passo 5 imprime `ok` em "ICMS do CSV × gfis2_ouro.g_arrecadacao". Um aviso
   aqui não aborta: confira se o CSV é do dia e decida.

## 7. Promover (servidor)

```bash
./gap_tributario/run_gap_job.sh carga_gold_gap.py promover
```

Antes de gravar, o job confere tudo e imprime o `snapshot_id` vigente de cada
tabela da ouro. Guarde esses números: são o rollback. Depois, no `impala-shell`,
rode os `INVALIDATE METADATA` que o job imprime e peça a alguém da SEFAZ o print
do painel.

Rollback de uma tabela da ouro:

```sql
CALL spark_catalog.system.rollback_to_snapshot('gfis2_ouro.g_gap_resultado', <snapshot_id>)
```

## Uma vez só: corrigir os COMMENTs da ouro

Os comentários antigos descrevem importações como FOB × PTAX do MDIC. A correção
é só de metadado e não toca coluna, tipo nem dado.

```bash
./gap_tributario/run_gap_job.sh corrige_comments_gold.py            # mostra os comandos
./gap_tributario/run_gap_job.sh corrige_comments_gold.py --aplicar
```

## Quando 2026 tiver VAB

O fluxo é o mesmo, com `--anos 2020-2026`. Duas coisas para resolver antes:

- o batch recusa `--vab-manual`, porque o override valeria para todos os anos.
  O VAB de 2026 precisa chegar pelo IMESC (CSV em `src/gap_tributario/data/`)
  ou pelo SIDRA;
- `flg_parcial` sai sempre `false`. Um 2026 incompleto precisa marcar a flag.
