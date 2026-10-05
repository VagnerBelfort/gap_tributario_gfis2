# gap_tributario_gfis2 — Calculadora de Gap Tributário ICMS-MA

## Propósito

CLI em Python que calcula o Gap Tributário do ICMS do Maranhão usando a
metodologia VAT Revenue Ratio (VRR) da OCDE. Entregável para a SEFAZ-MA,
projeto GFIS-2.

## Fórmula (não alterar sem aprovação metodológica)

```
Base       = VAB − Exportações + Importações
Potencial  = Base × Alíquota Padrão
VRR        = ICMS Arrecadado / Potencial
Gap        = Potencial − ICMS Arrecadado
```

Referência HISTÓRICA de validação MA 2022: VRR ≈ 0,518 (ICMS=10.917,
VAB=124.859, Exp=29.754, Imp=21.924, Alíq=0,18). Esses são os valores fixos dos
goldens de fórmula em `tests/test_engine/` — eles testam a aritmética, não as fontes.
O `Imp=21.924` vem da apresentação do 79º ENCAT e **não é uma apuração** (ver
pitfall abaixo); como entrada de golden ele continua válido, porque o teste é
da conta, não do dado.
Com as fontes atuais (ICMS do GFIS2, importações do Siscomex) o 2022 real dá
VRR ≈ 0,469 e gap ≈ R$ 12.892M. Não confundir os dois.

## Arquitetura — pipeline de 7 estágios

```
CLI Parse → Config → Extract → Validate → Calculate → Report → Output
```

Entrypoint: `python -m gap_tributario --periodo YYYY[-TN]`

Código em `src/gap_tributario/`:
- `extractors/` — uma classe por fonte; fonte indisponível levanta
  `ExtractionError` (`extractors/base.py`), e a cascata cai para a próxima
- `engine/vrr.py` — motor VRR (não mutar fórmula)
- `validators/schemas.py` — Pandera, fail-fast
- `report/` — PDF (reportlab) e Excel (xlsxwriter) com proveniência; `ouro.py`
  gera os CSVs das tabelas do Impala (`--exportar-ouro`)
- `cli.py` — orquestra os 7 estágios

## Fontes de dados — cascatas

Cada variável da fórmula tem uma cascata de fontes em ordem de prioridade.
A primeira fonte que responde com sucesso vence; falhas levam à próxima.

| Variável | Ordem da cascata |
|---|---|
| VAB | IMESC PIB Trimestral → IBGE SIDRA |
| Importações | Snapshot Siscomex → MDIC ComEx |
| Exportações | MDIC ComEx direto |
| ICMS Arrecadado | GFIS2 Parquet → SIGDEF (CONFAZ) |
| PTAX | BCB API Olinda (fonte única) |

`--vab-manual`, `--imp-manual` e `--exp-manual` são override: quando
informados, vencem a cascata da variável e valem para o **período pedido**
(num trimestre, o valor do trimestre).

A ordem vive em `cli.py`. O `config/fontes.yaml` ainda não é lido pelo código;
ele registra a ordem e o porquê de cada fonte, e muda junto com o `cli.py`.
Cada resultado carrega um objeto
`Proveniencia` que vira linha no relatório final.

## Pontos de atenção (dívida e pitfalls)

- **Importações vêm do Siscomex, não do MDIC**: o MDIC publica por UF do
  produto, que não separa domicílio fiscal de local de desembaraço; o ICMS de
  importação é devido no domicílio do importador. A separação correta é
  `TDS_UF_IMPORTADOR` no Siscomex. O MDIC serve como **validação cruzada**, não
  como fonte: 2019-2025, a nossa apuração (CIF, domicílio) fica de +2,4% a
  +12,8% acima do MDIC (FOB × PTAX), mediana +6,8% — a assinatura esperada de
  CIF sobre FOB. `engine/comparacao.py` aplica isso como controle a cada
  execução, distinguindo `FAIXA_OBSERVADA` (+2,4% a +12,8%, o que foi medido e
  o que o relatório exibe) de `CORREDOR_CONTROLE` (+2% a +13%, mais largo
  porque o extremo de 2022 é +2,368% e só arredonda para +2,4% na tela). Só
  vale para período anual. Tabela ano a ano em
  `docs/convergencia-importacoes.md` §3, refeita por
  `uv run python scripts/serie_mdic_ptax.py` (ComexStat + BCB, sem rede da SEFAZ).

- **O MDIC não superestima as importações do MA** — a intuição de que ele
  infla por incluir trânsito por Itaqui está medida e é falsa: o MDIC fica
  *abaixo* da nossa apuração em todos os anos de 2019 a 2025. O trânsito existe
  (~0,1% a 9,7% do valor despachado), mas o desconto do FOB frente ao CIF é
  maior que ele.

- **O filtro NCM cap.27/31 estava errado — não reativar**: a premissa era que
  esses capítulos seriam "quase tudo trânsito". Medido no Siscomex (2022,
  despachado no MA): **90% do cap.27 e 70% do cap.31 pertencem a importadores
  maranhenses**. Os capítulos dominam Itaqui (~92% do valor), mas dominância
  não é trânsito. O trânsito real é ~10-13% do valor despachado. O filtro
  descartava R$ 34,3 bi de importação legítima para remover R$ 4,6 bi de
  trânsito, e entregava R$ 3,3 bi em 2022 contra R$ 37,5 bi reais.
  `config/ncm_transito.yaml` segue no repo apenas como registro histórico.

- **Snapshot do Siscomex**: o Oracle C3 (`APL_SISCOMEX`) só responde de dentro
  da rede da SEFAZ. `scripts/snapshot_siscomex.py` roda no cluster e exporta um
  CSV **agregado** (ano × trimestre × capítulo NCM × UF despacho × UF
  importador) — sem CNPJ, sem nome de importador, sem nível de DI, por sigilo
  fiscal. Regras apuradas empiricamente e embutidas no job: chave composta
  `(NUM_DECL, SEQ_DECL)`; deduplicação pela última versão da DI (retificações);
  `TDS_SITUACAO = 'N'` **não** é cancelamento (essas DIs desembaraçam e pagam
  II/IPI, então entram); tipos de declaração `01` e nulo.

- **DUIMP soma com a DI a partir de nov/2025**: a DUIMP (Declaração Única de
  Importação, Portal Único) substitui a DI; cada importação está em uma ou na
  outra, nunca nas duas. Sem ela, o 1º semestre de 2026 sai 43% abaixo do MDIC.
  O mesmo job lê a DUIMP do **ARMA** (`10.1.1.132:1521/arma`, sinônimo `DUIMP`
  → `APL_PUCOMEX.DUIMP@CENTRAL`, criado pela SEFAZ em 21/09/2026). Pelo CENTRAL
  e pelo nome qualificado dá ORA-00942, e a `VW_CONSULTA_DUIMP` foi desativada
  pela SEFAZ. O CSV marca cada linha na coluna `fonte` (`DI` | `DUIMP`).
  Regras medidas em 23/09/2026 (`scripts/diagnostico_duimp_arma.py`):
  `STVIGENTE='S'` **não** deixa uma linha por DUIMP (228 têm mais de uma
  versão vigente); vale a maior `VERSAODECLARACAO`, e somar todas levava
  2025-26 de R$ 12,9 bi para R$ 22,9 bi. O valor é `VLMERCADORIALOCALDESCARGAREAL`
  (CIF). A tabela não tem data de desembaraço, então o período sai da **data
  mais tardia entre `DATAHORAREGISTRO` e `DATACHEGADA`**. A chegada vem de
  `APL_PUCOMEX.CARGA@CENTRAL`, lida do ARMA. Foi a SEFAZ (Alan Lima, TI) que
  indicou `DATACHEGADA` como a data de liberação e liberou o dblink em
  28/09/2026, com join pelo `IDDUIMP` da versão escolhida, porque o
  `IDDUIMP` identifica a versão. Há uma carga por DUIMP, e chegada nula vale o
  registro (há R$ 1,2 bi desembaraçados sem ela). Essa regra moveu R$ 357 mi
  de 2025 T4 para 2026 T1. `IDUFIMPORTADOR` é o índice alfabético da UF
  (10 = MA, conferido com o cadastro). Não tem NCM nem UF de despacho.
  **Todas as `IDSITUACAODUIMP` entram**: na base aparecem 5, 6, 8 e 10
  (registrada ou em conferência) e 11, 12 e 13 (desembaraçada), e nenhuma
  cancelada (22, 23). A situação 5 (~R$ 2,6 bi, "aguardando análise de
  risco") não anda na cópia da SEFAZ e 76% dela tem a carga chegada, então são
  importações reais. Sem ela, o 1º sem/2026 cai para −7,6% do MDIC. Medido em
  `scripts/diagnostico_duimp_carga.py` (28/09/2026).

- **`TDS_SITUACAO` (S/N) — confirmado pela SEFAZ, não filtrar**: em 24/08/2026
  a SEFAZ-MA (Alan Lima, TI) respondeu por e-mail que o campo "vem diretamente
  da Receita, não é utilizado aqui e pode ser desconsiderado". Todas as DIs
  desembaraçadas entram (R$ 12,5 bi em 2022). Não construir flag "só S" no CLI.

- **Os R$ 21,9 bi do 79º ENCAT não são uma apuração**: em 24/08/2026 o autor
  do cálculo (Jomar, SEFAZ-MA) informou por áudio que o valor era "só uma
  referência" para a apresentação, que o dado do sistema deles para 2022 é
  R$ 38,7 bi e que a fonte é o MDIC. Não há memória de cálculo a reproduzir —
  a caça à definição equivalente está encerrada. Transcrição em
  `resposta_jomar/transcricao.md`; `scripts/reconciliar_encat_2022.py` segue no
  repo como registro histórico.

- **UF nula resolvida pelo cadastro**: DIs sem `TDS_UF_IMPORTADOR` são
  atribuídas via `gfis2_bronze.b_contribuinte.uf_icms` (join por CNPJ de 14
  dígitos). Usar `uf_icms`, **não** `uf` — esta é nula em 100% das linhas com
  CNPJ. Validação: dos 797 CNPJs que o Siscomex marca como MA, os 673 que
  casaram vieram todos MA (zero falso positivo); dos 855 marcados como outra
  UF, o cadastro disse MA em 12 (~9%), provavelmente substituto tributário com
  inscrição estadual no MA. O viés empurra levemente para cima.

- **Assimetria CIF × FOB na base**: `Base = VAB − Exportações + Importações`
  hoje mistura conceitos — importações vêm do Siscomex em **CIF** (inclui frete
  e seguro), exportações vêm do MDIC em **FOB**. Isso infla a perna de
  importação em ~10-15% frente à de exportação. O ComexStat não publica CIF por
  UF; a razão limpa entre os dois conceitos sai do próprio Siscomex, que traz
  valor da mercadoria e valor aduaneiro na mesma DI. Registrar na proveniência.

  Já a assimetria de *critério geográfico* foi auditada e **não** existe: o
  ComexStat usa estado produtor nas exportações e origem/destino declarada nas
  importações. Teste de magnitude: o MA exportou US$ 5,74 bi em 2022, enquanto
  o minério de Carajás escoado por Ponta da Madeira sozinho passa de US$ 15 bi
  — ele é atribuído ao Pará, então o trânsito não contamina a exportação.

- **Janela suportada**: o Siscomex só é aceito de **2013** em diante
  (`_ANO_COBERTURA_CONFIAVEL`). Antes disso a base tem 205 DIs em 2011 e 1.808
  em 2012, contra ~3.000/ano depois — somar o que existe devolveria um total
  muito abaixo do real com cara de número válido. Anos anteriores levantam
  `ExtractionError` e caem para o MDIC. O limite de 2013 tem confirmação
  externa: em 2011 e 2012 o Siscomex fica 76% e 44% *abaixo* do MDIC, contra o
  corredor de +2% a +13% de 2013 em diante. Calibrar o MDIC para cobrir
  2011-2012 foi **descartado** — extrapolar um fator medido fora do período não
  se sustenta, e o limite real do produto é a arrecadação: **o escopo de
  análise é 2019 em diante**.

- **Balde `NI`**: hoje 30 DIs e R$ 9,1 mi (era R$ 2,23 bi antes da resolução por
  CNPJ). Excluídas por padrão; `--imp-incluir-ni` dá o teto da sensibilidade.

- **Anos inválidos na origem**: há DIs com ano de desembaraço digitado errado
  (18, 202, 203...), ~R$ 0,04 bi. O job descarta com contagem explícita.

- **VAB vem do IMESC, trimestral nativo**: `extractors/imesc_pib.py` lê
  `src/gap_tributario/data/imesc_pib_trimestral_ma.csv`, transcrito da
  Tabela 15 (valores correntes) de
  `docs/Relatorio-Especializado-do-PIB-Trimestral-1.pdf` (edição do 4º
  tri/2025). Cobre 2021 T1 a 2025 T4; o trimestre usa o valor direto, o ano
  soma os quatro. A soma de 2022 dá 124.859, o mesmo VAB do IBGE usado nos
  goldens.

- **2026 não tem VAB (vedação eleitoral do IMESC)**: o IMESC publicou o 1º
  tri/2026 em 02/07/2026 e arquivou o site em 04/07/2026; os arquivos em
  `wp-content/uploads` redirecionam para a home e não há cópia em arquivos da
  web. Sem `--vab-manual`, qualquer período de 2026 (ano ou trimestre)
  termina em erro: o IMESC não cobre e o SIDRA ainda não publicou.

- **IBGE SIDRA cobre o que o IMESC não cobre**: o CLI cai para ele sempre que
  o IMESC falha. Como o ICMS começa em 2020, na prática só atende 2020.
  Contas Regionais (tabela 5938) publica o ano N no final do ano N+2. A
  variável 37 é PIB, não VAB; aplicamos `VAB = PIB × 0.8932`, razão das
  Contas Regionais de 2022 (o IMESC dá a mesma em 2022, mas ~0,85 em
  2024-2025, quando os impostos passam a pesar mais). No trimestre, o
  extrator divide o ano por 4.

- **Alíquota mudou em 2023**: Lei 11.867/2022 elevou de 18% para 20%.
  Configurado em `config/aliquotas.yaml`. Mudanças mid-year não são
  suportadas hoje — usar o ano inteiro com a alíquota vigente.

- **A série de ICMS começa em 2020**: o GFIS2 só entra em regime completo em
  agosto/2019 — o ano de 2019 subestima o arrecadado e produzia o VRR 0,213
  antes marcado como suspeito (causa confirmada em 2026-08). Não publicar
  anos anteriores a 2020 com ICMS do GFIS2.

- **ICMS soma todas as parcelas**: `extractors/arrecadacao.py` agrega todas as
  colunas de parcela do GFIS2. A composição antiga (`normal + imp + st_sda`)
  subestimava 10-15% — faltavam FCP e dívida ativa. Validação: GFIS2 ×
  SIGDEF/CONFAZ fecha dentro de ~1% em 2020-2023 (era −10% a −15% antes).
  O SIGDEF é fallback da cascata, não fonte primária.

- **Publicação no Impala — a ouro é contrato do painel**: o painel da SEFAZ lê
  `gfis2_ouro.g_gap_*`. Colunas, tipos, ordem e valores categóricos (`fonte_*`,
  `variavel`, `componente`) são os da carga de 31/08/2026 e ficam congelados; uma
  carga nova muda só valores. O CLI calcula e exporta CSVs (`report/ouro.py`),
  `jobs/carga_gold_gap.py` carrega no `gfis2_dev` com as travas de
  `jobs/carga_gold_regras.py`, e a produção recebe por cópia do dev (`promover`),
  nunca por carga direta. O `snapshot_siscomex.py` grava junto a bronze
  (`b_siscomex_di`, `b_duimp`, `b_duimp_carga`, todas as versões) e a prata
  (`s_gap_importacoes`). Os jobs rodam no Python 3.6.8 do driver. Trimestre só
  sai com VAB trimestral do IMESC (2020 não tem). O painel HML lê o
  `gfis2_dev`; o de produção, a `gfis2_ouro`. O usuário `gfis2` tem limite de
  logins simultâneos: uma sessão SSH persistente, jobs com `setsid nohup`.
  Passo a passo, acesso e histórico de cargas em `docs/runbook-carga-impala.md`.

## Comandos principais

```bash
uv sync                                      # instala deps
uv run python -m gap_tributario --periodo 2022
uv run python -m gap_tributario --periodo 2022-T1 --formato pdf excel
uv run python -m gap_tributario --periodo 2026-T1 --vab-manual <VAB_do_trimestre>
uv run python -m gap_tributario --periodo 2022 --imp-incluir-ni  # sensibilidade NI
uv run python -m gap_tributario --exportar-ouro output/ouro --anos 2020-2025  # CSVs do Impala

# Regerar o snapshot e carregar no Impala (rede da SEFAZ): docs/runbook-carga-impala.md
uv run pytest tests/ --cov=src/gap_tributario
uv run ruff check src/
uvx vermin --no-tips -t=3.6- jobs/ scripts/snapshot_siscomex.py  # driver do cluster é 3.6
```

## Workflow de desenvolvimento (obrigatório)

1. Antes de qualquer mudança: invocar `Skill superpowers:test-driven-development`
2. Antes de debugar: invocar `Skill superpowers:systematic-debugging`
3. Antes de declarar pronto: invocar `Skill superpowers:verification-before-completion` — rodar testes e colar output
4. Antes de PR: invocar `Skill code-review:code-review` no PR criado

Golden tests são intocáveis: se quebrarem, a fórmula ou a metodologia mudou —
pedir revisão metodológica antes de atualizar o Golden. Não existe
`tests/test_golden.py`: os goldens da fórmula vivem em `tests/test_engine/` e
`tests/test_integration.py`, com entradas fixas (por isso trocar a fonte de
importações não os quebra). O golden da fonte fica em
`tests/test_extractors/test_siscomex_snapshot.py`.

## Testes — critérios de aceitação

- `pytest tests/` verde
- Cobertura ≥ 80% total, ≥ 85% em módulos novos
- `ruff check src/` limpo
- Golden 2022 (VRR=0,518 ± 0,002) passa
- Golden Siscomex: importações 2022 = R$ 39.704M (± R$ 1M) a partir de
  `bases/siscomex_importacoes.csv`; pula se o snapshot não estiver presente
- Travas da carga do Impala (`tests/test_jobs/`) e cabeçalhos da ouro
  (`tests/test_report/test_ouro.py`, transcritos do DDL de produção) passam

## Skills recomendadas (Claude Code)

- `pyright-lsp` — navegação de símbolos Python
- `claude-md-management` — manutenção deste arquivo
- `superpowers:test-driven-development` — TDD obrigatório
- `superpowers:systematic-debugging` — root-cause, não patch
- `superpowers:verification-before-completion` — evidência antes de "pronto"
- `code-review:code-review` — review de PR

## Dependências-chave

Python 3.8+, `uv` para gestão; a lista está no `pyproject.toml`. O pacote não
fala com Oracle: o acesso ao `APL_SISCOMEX` vive em
`scripts/snapshot_siscomex.py`, que roda com `spark3-submit` no cluster da
SEFAZ e não é dependência do CLI.

## Referências

- OCDE VRR methodology: Consumption Tax Trends 2020, Chapter 2
- IMESC PIB Trimestral do Maranhão: https://imesc.ma.gov.br/ (arquivado
  durante a vedação eleitoral de 2026)
- IBGE Contas Regionais, Tabela 5938
- MDIC ComexStat: https://balanca.economia.gov.br/
- BCB PTAX: https://olinda.bcb.gov.br/olinda/servico/PTAX
- Legislação: Lei Estadual 11.867/2022 (alíquota 20%)
- Material interno: `docs/63_Texto_do_artigo_216_1_10_20200316.pdf`
  e `docs/Apresentação - 79 encat-Gap do ICMS-VAT_VRR.pptx`
- Spec de melhorias: `docs/superpowers/specs/2026-04-14-avaliacao-gap-tributario-design.md`
