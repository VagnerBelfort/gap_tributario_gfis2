# Runbook — carga do Gap Tributário no Impala

Publica as importações (DI + DUIMP) e o gap de 2020 a 2025 nas tabelas que o
painel da SEFAZ lê: todos os anos e os trimestres com VAB trimestral do IMESC
(2021 T1 em diante; os de 2020 são pulados com aviso, porque o VAB do SIDRA no
trimestre é o ano dividido por 4). Uma extração alimenta tudo: o CSV do CLI, a prata e o
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

## Acesso ao servidor

- **O usuário `gfis2` tem limite de logins simultâneos.** Com uma sessão já
  aberta, uma segunda é recusada logo depois da autenticação ("Connection
  closed by … port 22", com a chave aceita). Use uma sessão só; quem roda pelo
  agente abre uma conexão persistente e passa tudo por ela:
  `ssh -o ControlMaster=auto -o ControlPath=/tmp/ssh-sefaz-%r@%h -o ControlPersist=3h gfis2@Sefazbige01`.
  Insistir em reconexões seguidas arrisca bloquear a conta.
- **A senha do Oracle fica em `~/.siscomex_oracle`** (`chmod 600`, criado pelo
  operador com `umask 077`), no formato
  `export SISCOMEX_ORACLE_PASSWORD='...'`, entre aspas simples. Nunca no chat,
  no repositório ou na linha de comando.
- **Jobs longos rodam desacoplados da sessão**, para sobreviver a queda de VPN
  ou de SSH: `setsid nohup bash ./gap_tributario/run_gap_job.sh … > gap_tributario/<job>_AAAAMMDD.out 2>&1 < /dev/null &`.
  O SSH que dispara pode ficar preso mesmo assim; encerre-o (o job continua) e
  acompanhe pelo log e por `pgrep -af <job>`.
- **Comandos `hdfs dfs` com `timeout`** (`timeout 150 hdfs dfs -put …`): um
  `put` já travou indefinidamente. O aviso `StandbyException` é o cliente
  tentando primeiro o namenode em standby; é normal.
- Impala: `impala-shell -i sefazbige02.sefaz.ma.gov.br -d default -k --ssl --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem`
  (`--output_delimiter` aceita um caractere só).

## 0. Levar o código ao servidor

```bash
scp scripts/snapshot_siscomex.py scripts/export_arrecadacao_agregada.py \
    jobs/carga_gold_gap.py jobs/carga_gold_regras.py jobs/corrige_comments_gold.py \
    jobs/run_gap_job.sh \
    gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/
```

## 1. Extração (servidor)

```bash
source ~/.siscomex_oracle
setsid nohup bash ./gap_tributario/run_gap_job.sh snapshot_siscomex.py \
    --saida /gfis2/pipeline/gap_tributario/siscomex_importacoes_AAAAMMDD.csv \
    > gap_tributario/snapshot_AAAAMMDD.out 2>&1 < /dev/null &
```

Grava `gfis2_dev.b_siscomex_di`, `gfis2_dev.b_duimp`, `gfis2_dev.b_duimp_carga`,
`gfis2_dev.s_gap_importacoes` e o CSV (com a data no nome, sem sobrescrever o
anterior). Confira no log as quatro linhas `[GRAVADO]` e o total por ano e
fonte: anos fechados devem repetir a extração anterior.

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
scp gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/siscomex_importacoes_AAAAMMDD.csv \
    bases/siscomex_importacoes.csv
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
3. O passo 5 imprime `26`, `24` e `182` linhas (resultado, decomposição,
   proveniência): 6 anos + 20 trimestres; decomposição só nos anos com AMF;
   7 variáveis por período. Nos trimestres, VAB, importações e ICMS somam o
   ano; as exportações diferem em até ~0,4%, porque cada trimestre usa a PTAX
   do próprio trimestre.
4. O passo 5 imprime `ok` em "Importações do CSV × soma MA de gfis2_dev.s_gap_importacoes".
5. O passo 5 imprime `ok` em "ICMS do CSV × gfis2_ouro.g_arrecadacao". Um aviso
   aqui não aborta: confira se o CSV é do dia e decida.
6. **A tela de homologação mostra a carga do dev.** O painel HML
   (`gfis2-web-app-hml.sefaz.ma.gov.br`, Gap Tributário › Visão Geral) lê o
   `gfis2_dev`, então exibe a carga logo depois do passo 5: é a conferência
   visual antes de promover. O print do HML não valida a produção.

## 7. Promover (servidor)

```bash
./gap_tributario/run_gap_job.sh carga_gold_gap.py promover
```

Antes de gravar, o job confere tudo e imprime o `snapshot_id` vigente de cada
tabela da ouro. Registre esses números no histórico abaixo: são o rollback.
Depois, no `impala-shell`, rode os `INVALIDATE METADATA` que o job imprime e
peça a alguém da SEFAZ o print do painel de produção.

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

## Histórico de cargas

Os `snapshot_id` são o estado da ouro **antes** de cada promoção: é para eles
que o rollback daquela carga volta.

| Data | O que subiu | Snapshot | ICMS (corte) | `snapshot_id` anteriores (resultado / decomposição / proveniência) |
|---|---|---|---|---|
| 31/08/2026 | anos 2020-2025, só DI (`CARGA_MANUAL_2026-08-31`, seed) | anterior à DUIMP | — | — |
| 05/10/2026 | anos 2020-2025, DI + DUIMP (`CARGA_CLI_2026-10-02`) | 02/10/2026 | 02/10 | `4957716316270788721` / `5916263664551558192` / `1547875153688911529` |
| 05/10/2026 | anos 2020-2025 + trimestres 2021 T1-2025 T4 (`CARGA_CLI_2026-10-05`) | 02/10/2026 | 02/10 | `5241182302227105531` / `4670815801054243773` / `8047630858766791194` |

Na carga vigente: `g_gap_resultado` 26 linhas, `g_gap_decomposicao` 24,
`g_gap_proveniencia` 182; bronze `b_siscomex_di` 124.124, `b_duimp` 2.410,
`b_duimp_carga` 2.410; prata `s_gap_importacoes` 9.319.
