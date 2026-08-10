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
goldens de fórmula em `tests/engine/` — eles testam a aritmética, não as fontes.
Com as fontes atuais (ICMS do SIGDEF, importações do Siscomex) o 2022 real dá
VRR ≈ 0,473 e gap ≈ R$ 12.792M. Não confundir os dois.

## Arquitetura — pipeline de 7 estágios

```
CLI Parse → Config → Extract → Validate → Calculate → Report → Output
```

Entrypoint: `python -m gap_tributario --periodo YYYY[-TN]`

Código em `src/gap_tributario/`:
- `extractors/` — uma classe por fonte, contrato `FonteExtractor` Protocol
- `engine/vrr.py` — motor VRR (não mutar fórmula)
- `engine/seasonality.py` — rateio sazonal trimestral
- `validators/schemas.py` — Pandera, fail-fast
- `report/` — PDF (typst-py) e Excel (xlsxwriter) com proveniência
- `cli.py` — orquestra os 7 estágios

## Fontes de dados — cascatas

Cada variável da fórmula tem uma cascata de fontes em ordem de prioridade.
A primeira fonte que responde com sucesso vence; falhas levam à próxima.

| Variável | Ordem da cascata |
|---|---|
| VAB | IBGE SIDRA → IPEADATA → BCB Focus → AutoARIMA forecast → `--vab-manual` |
| Importações | Snapshot Siscomex → MDIC ComEx → `--imp-manual` |
| Exportações | MDIC ComEx direto → `--exp-manual` |
| ICMS Arrecadado | GFIS2 Parquet (fonte única) |
| PTAX | BCB API Olinda (fonte única) |

Ordem configurável em `config/fontes.yaml`. Cada resultado carrega um objeto
`Proveniencia` que vira linha no relatório final.

## Pontos de atenção (dívida e pitfalls)

- **Importações vêm do Siscomex, não do MDIC**: o `SG_UF_NCM` do MDIC é local
  de desembaraço, não domicílio fiscal — confirmado empiricamente, porque o FOB
  do MDIC para o MA em 2022 (cap.27 R$ 27,2 bi; cap.31 R$ 8,7 bi) só fica
  *abaixo* do CIF do Siscomex quando comparado ao universo "despachado no MA",
  e FOB não pode exceder CIF do mesmo conjunto. A separação correta é
  `TDS_UF_IMPORTADOR` no Siscomex.

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
  importação em ~10-15% frente à de exportação. Não há correção simples: o
  ComexStat não publica CIF por UF. Registrar na proveniência.

  Já a assimetria de *critério geográfico* foi auditada e **não** existe: o
  ComexStat usa estado produtor nas exportações e origem/destino declarada nas
  importações. Teste de magnitude: o MA exportou US$ 5,74 bi em 2022, enquanto
  o minério de Carajás escoado por Ponta da Madeira sozinho passa de US$ 15 bi
  — ele é atribuído ao Pará, então o trânsito não contamina a exportação.

- **Balde `NI`**: hoje 30 DIs e R$ 9,1 mi (era R$ 2,23 bi antes da resolução por
  CNPJ). Excluídas por padrão; `--imp-incluir-ni` dá o teto da sensibilidade.

- **Anos inválidos na origem**: há DIs com ano de desembaraço digitado errado
  (18, 202, 203...), ~R$ 0,04 bi. O job descarta com contagem explícita.

- **IBGE SIDRA tem lag de ~2 anos**: Contas Regionais (tabela 5938) publica
  o ano N no final do ano N+2. Para 2024-2026 usar a cascata de fallback.

- **Fator PIB→VAB**: SIDRA 5938 variável 37 retorna PIB, não VAB. Aplicamos
  `VAB = PIB × 0.8932` (base: Contas Regionais IBGE 2022). Esse fator é
  aproximação — validar contra série VAB direta quando disponível.

- **Sazonalidade trimestral**: O VAB é anual. Para períodos trimestrais
  `engine/seasonality.py` rateia pelo peso do ICMS do próprio ano:
  `VAB_Tn = VAB_anual × (ICMS_Tn / ICMS_anual)`. Premissa: VRR ≈ constante
  intra-ano (aceitável OCDE para horizontes curtos).

- **Alíquota mudou em 2023**: Lei 11.867/2022 elevou de 18% para 20%.
  Configurado em `config/aliquotas.yaml`. Mudanças mid-year não são
  suportadas hoje — usar o ano inteiro com a alíquota vigente.

- **VRR 2019 = 0,213 é suspeito**: comparar com outras UFs e validar se
  o Parquet GFIS2 tem dados completos para 2019.

## Comandos principais

```bash
uv sync                                      # instala deps
uv run python -m gap_tributario --periodo 2022
uv run python -m gap_tributario --periodo 2022-T1 --formato pdf excel
uv run python -m gap_tributario --periodo 2024 --vab-manual 140000
uv run python -m gap_tributario --periodo 2022 --imp-incluir-ni  # sensibilidade NI

# Regerar o snapshot de importações (dentro da rede da SEFAZ):
#   scp scripts/snapshot_siscomex.py gfis2@Sefazbige01:/gfis2/pipeline/gap_tributario/
#   spark3-submit --master yarn --deploy-mode client --jars /gfis2/jars/ojdbc8.jar \
#     gap_tributario/snapshot_siscomex.py --saida .../siscomex_importacoes.csv
#   scp gfis2@Sefazbige01:.../siscomex_importacoes.csv bases/
uv run pytest tests/ --cov=src/gap_tributario
uv run ruff check src/
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

## Skills recomendadas (Claude Code)

- `pyright-lsp` — navegação de símbolos Python
- `claude-md-management` — manutenção deste arquivo
- `superpowers:test-driven-development` — TDD obrigatório
- `superpowers:systematic-debugging` — root-cause, não patch
- `superpowers:verification-before-completion` — evidência antes de "pronto"
- `code-review:code-review` — review de PR

## Dependências-chave

Python 3.8+, `uv` para gestão. Core: `polars`, `pandera`, `httpx`,
`sidrapy`, `python-bcb`, `ipeadatapy`, `basedosdados`, `statsforecast`,
`pointblank`, `typst-py`, `reportlab` (legado, em migração), `xlsxwriter`,
`pyyaml`. O pacote não fala com Oracle: o acesso ao `APL_SISCOMEX` vive em
`scripts/snapshot_siscomex.py`, que roda com `spark3-submit` no cluster da
SEFAZ e não é dependência do CLI.

## Referências

- OCDE VRR methodology: Consumption Tax Trends 2020, Chapter 2
- IBGE Contas Regionais, Tabela 5938
- MDIC ComexStat: https://balanca.economia.gov.br/
- BCB PTAX: https://olinda.bcb.gov.br/olinda/servico/PTAX
- Legislação: Lei Estadual 11.867/2022 (alíquota 20%)
- Material interno: `docs/63_Texto_do_artigo_216_1_10_20200316.pdf`
  e `docs/Apresentação - 79 encat-Gap do ICMS-VAT_VRR.pptx`
- Spec de melhorias: `docs/superpowers/specs/2026-04-14-avaliacao-gap-tributario-design.md`
