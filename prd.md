# PRD — Integração de novas fontes: VAB real, ICMS corrigido e decomposição do gap

> Status: ready-for-agent · Projeto GFIS-2 · Entregável SEFAZ-MA
> Origem: consolidado via `/grill-me` em 2026-06-08

## Problem Statement

Hoje a Calculadora de Gap Tributário ICMS-MA não consegue calcular bem os
períodos recentes (2024–2026) porque a fonte de VAB (IBGE SIDRA, tabela 5938)
tem lag de ~2 anos e exige a aproximação `VAB = PIB × 0,8932`. Além disso:

- A sazonalidade trimestral é apenas **assumida** (premissa "VRR constante
  intra-ano"), sem dado real.
- O "Gap" entregue é um **número único e opaco** (`Potencial − Arrecadado`),
  que mistura renúncia fiscal legal com evasão — a SEFAZ não consegue saber
  quanto do gap é política tributária (benefício concedido por lei) e quanto
  é sonegação.

O usuário recebeu três novos conjuntos de dados oficiais que, bem integrados,
resolvem essas três lacunas.

## Solution

Integrar três fontes novas, cada uma com um papel distinto, mantendo a fórmula
VRR nuclear intocada:

1. **VAB real (IMESC)** — passa a ser a fonte nº 1 da cascata de VAB, com VAB
   trimestral nominal direto, eliminando a aproximação PIB×fator, o lag IBGE e
   a premissa de sazonalidade para os anos cobertos.
2. **ICMS corrigido (SIGDEF)** — fonte primária de ICMS arrecadado, lida com
   correção do desalinhamento de colunas do export.
3. **Decomposição do gap (AMF Tabela 7)** — novo estágio que separa o gap total
   em *policy gap* (renúncia legal) e *compliance gap* (evasão), padrão RA-GAP
   do FMI, com novas linhas no relatório.

### Fontes e papéis

| Fonte | É | Papel |
|---|---|---|
| `Relatorio-Especializado-do-PIB-Trimestral-1.pdf` (IMESC/Gov MA) | VAB+PIB+Impostos trimestral MA, **valores correntes**, por setor, 2021–2025 | Preenche a lacuna real de VAB |
| `docs/20260204_por-setor.xls` (CONFAZ/SIGDEF) | ICMS arrecadado mensal por setor, todas UFs, 1997–2023, nominal | Fonte correta de ICMS |
| `docs/AMF-Tabela 7 ...` (LDO 2022/2025/2026, ×3) | Renúncia fiscal MA (BI-Oracle-SEFAZ), por modalidade/benefício, 2022–2029 | Decompõe o gap |
| `docs/63_Texto_do_artigo_216...pdf` | Paper CGE de federalismo fiscal | **Fora de escopo** — só referência metodológica |

### Validações de ancoragem (já realizadas)

- **IMESC Tabela 15 (correntes):** soma dos 4 trimestres do VAB 2022 =
  `25.871 + 33.980 + 31.834 + 33.174 = 124.859` = **exatamente o golden**.
  Identidade `PIB = VAB(preços básicos) + Impostos(IPLS)` confere.
- **SIGDEF (offset +1):** a coluna de posição 21 (rotulada erroneamente
  `va_icms_outras`) é o ICMS total real → MA 2022 = R$ 11.494 mi; nacional
  2022 = R$ 691 bi (~ICMS Brasil real). A coluna rotulada `va_icms_total` está
  errada (R$ 681 mi, ~16× baixa).
- **Decomposição golden 2022:** Gap Total `21.065 − 10.917 = 10.148` =
  Policy `2.182` (renúncia ICMS 2022) + Compliance `7.966` (~78,5%).

## User Stories

1. Como analista da SEFAZ-MA, quero calcular o gap de 2024–2026 com VAB real do
   IMESC, para não depender da aproximação PIB×0,8932 nem do lag de 2 anos do IBGE.
2. Como analista, quero que o VAB venha direto da Tabela 15 (valores correntes)
   do relatório IMESC, para que ele seja compatível (nominal) com o ICMS nominal
   na fórmula VRR.
3. Como analista, quero que o sistema **nunca** use a Tabela 16 (preços de 2010)
   no cálculo, para não misturar valores reais com nominais e distorcer o VRR.
4. Como analista, quero que o IMESC seja a fonte nº 1 da cascata de VAB, com o
   SIDRA caindo para fallback nos anos não cobertos (<2021), para ter o melhor
   dado disponível por período.
5. Como analista, quero calcular um trimestre (ex.: `2024-T2`) usando o VAB real
   daquele trimestre, sem rateio, para que o número trimestral seja dado e não
   premissa.
6. Como analista, quero que o período anual seja a soma dos 4 trimestres do IMESC,
   para consistência entre as visões anual e trimestral.
7. Como analista, quero que o VRR trimestral também use Exp/Imp trimestralizados
   (ComEx mensal agregado ao trimestre), para um cálculo trimestral coerente
   ponta a ponta.
8. Como analista, quero usar o ICMS arrecadado do arquivo SIGDEF como fonte
   correta, porque é o arquivo que a SEFAZ usa para gerar o cálculo oficial.
9. Como desenvolvedor, quero que o leitor do SIGDEF corrija o desalinhamento de
   colunas (offset +1) lendo por posição, e não pelos rótulos, porque os rótulos
   do export estão trocados.
10. Como desenvolvedor, quero materializar o SIGDEF corrigido em um parquet limpo,
    para não reparsear o `.xls` (2,5 MB) a cada execução.
11. Como analista, quero que o teste golden 2022 continue ancorado na fonte antiga
    (GFIS2) até a revisão metodológica dos ~5%, para que adotar o SIGDEF não
    quebre o golden agora.
12. Como gestor da SEFAZ, quero ver o gap **decomposto** em policy gap (renúncia
    legal) e compliance gap (evasão), para saber quanto do gap é decisão de
    política e quanto é sonegação.
13. Como gestor, quero ver a renúncia detalhada por modalidade (Crédito Presumido,
    Isenção, Redução de Base de Cálculo), para entender a composição do policy gap.
14. Como gestor, quero que a decomposição cubra inclusive 2022 (ano golden),
    porque há dado de renúncia de 2022 nas planilhas da LDO-2022.
15. Como analista, quero que cada fonte nova carregue um objeto `Proveniencia`
    (origem, data de extração, observações), que vira linha no relatório, para
    rastreabilidade e auditoria.
16. Como leitor do relatório, quero caveats explícitos: renúncia é estimativa LDO
    (prospectiva, não realizada); VAB IMESC só cobre ≥2021; alíquota muda em 2023
    (18%→20%), para interpretar os números corretamente.
17. Como analista, quero que a fórmula VRR nuclear (`MotorVRR`) permaneça
    inalterada, para não introduzir risco metodológico.
18. Como desenvolvedor, quero a ordem das cascatas configurável em
    `config/fontes.yaml`, para ajustar prioridades sem mexer no código.
19. Como gestor, quero que o relatório PDF e o Excel mostrem as novas linhas
    (decomposição + proveniência), para uma entrega completa.
20. Como desenvolvedor, quero que `pytest` continue verde, cobertura ≥80% total e
    ≥85% nos módulos novos, e `ruff` limpo, para manter os critérios de aceitação.

## Implementation Decisions

### Novos módulos / interfaces

- **`SigdefParser` (transform puro de ingestão)** — função `parse_sigdef(raw) ->
  clean_df` que aplica o **fix de offset +1** (lê a coluna de posição 21 como
  ICMS total, ignorando os rótulos errados do export), valida sanidade (nacional
  ~ICMS Brasil) e produz um DataFrame limpo materializado em parquet. É o módulo
  profundo mais arriscado; isolado para teste com fixture pequena.
- **`SigdefIcmsExtractor` (Extractor)** — `extract(periodo) -> pl.DataFrame`; lê o
  parquet limpo, agrega mês→período (trimestre ou ano). Fonte primária de ICMS.
- **`ImescPibExtractor` (Extractor)** — `extract(periodo) -> pl.DataFrame` com VAB
  (e Impostos/PIB) correntes; soma os 4 trimestres no anual, retorna o trimestre
  direto no trimestral. Fonte nº 1 da cascata de VAB. Dados transcritos da
  Tabela 15 para arquivo versionado no repo.
- **`AmfRenunciaReader` (transform puro)** — `renuncia_icms(ano) -> Decimal` e
  detalhamento por modalidade; mescla os 3 xlsx (cobertura 2022–2029), seleciona
  o ano e soma as modalidades de ICMS (exclui IPVA).
- **`GapDecomposer` (`engine/gap_decomposition.py`, função pura)** — recebe
  `ResultadoGap` + renúncia do ano e retorna a decomposição:
  `policy_gap = renúncia`; `compliance_gap = gap_absoluto − policy_gap`; mais
  percentuais. **Não** toca `MotorVRR`.
- **`Proveniencia` (`models.py`, dataclass)** — origem, data de extração, fonte,
  observações; anexada aos dados extraídos e renderizada no relatório.

### Decisões arquiteturais

- Contrato real dos extratores é a classe-base **`Extractor`** (não um Protocol).
  Novos extratores herdam dela e implementam `extract`.
- **`config/fontes.yaml` (novo)** define a ordem das cascatas. VAB: IMESC →
  SIDRA → IPEADATA → BCB Focus → AutoARIMA → `--vab-manual`. ICMS: SIGDEF → GFIS2.
- **Pinagem do golden:** o teste golden 2022 permanece alimentado pela fonte GFIS2
  até a revisão metodológica dos ~5% (SIGDEF daria 11.494 vs golden 10.917). O
  SIGDEF é fonte primária para todos os demais usos.
- **Sem `engine/seasonality.py`:** o módulo descrito no CLAUDE.md não existe e
  **não** será criado; o caminho trimestral usa VAB IMESC direto, sem rateio.
- A fórmula VRR (`engine/vrr.py` / `MotorVRR`) permanece **intocada**. A
  decomposição é um estágio posterior, aditivo.
- `ResultadoGap` (frozen) ganha um companion de decomposição (policy/compliance)
  consumido pelo relatório; ou um novo `ResultadoGapDecomposto` que envelopa o
  `ResultadoGap` — a definir na issue, sem alterar a semântica do `ResultadoGap`.

### Contratos de dado

- VAB, ICMS, Exp, Imp em **R$ milhões**, **nominais/correntes**.
- IMESC: `PIB = VAB(preços básicos) + Impostos(IPLS)`; usar a coluna **VAB**.
- SIGDEF: ICMS total = coluna de **posição 21** (por posição, não por rótulo).
- AMF: somar modalidades de **ICMS** (Crédito Presumido + Isenção + Redução de
  Base de Cálculo); IPVA fica de fora do policy gap de ICMS.

## Testing Decisions

Bom teste = verifica **comportamento externo** (entrada→saída do módulo), não
detalhes de implementação. Usar fixtures pequenas e determinísticas.

Módulos com testes (decisão do usuário — todos os 4 transforms puros):

- **`SigdefParser`** — fixture com poucas linhas reproduzindo o offset +1;
  asserir que a col. 21 vira ICMS total e que a sanidade nacional bate a ordem
  de grandeza. Cobrir o caso de rótulos enganosos.
- **`GapDecomposer`** — casos puros: golden 2022 (10.148 = 2.182 + 7.966);
  renúncia > gap (compliance negativo → tratar/avisar); renúncia ausente para o
  ano (degradar para "só gap total").
- **`ImescPibExtractor`** — anual = soma dos 4 trimestres (2022 → 124.859);
  trimestral retorna o trimestre direto (2022-T1 → 25.871); ano fora de cobertura
  → sinaliza para fallback.
- **`AmfRenunciaReader`** — merge dos 3 xlsx; seleção por ano (2022 → 2.182 mi de
  ICMS); soma correta das modalidades; exclusão de IPVA.

Invariantes que **não** podem quebrar:

- **Golden 2022** (VRR = 0,518 ± 0,002) continua passando (ancorado em GFIS2).
- `pytest` verde; cobertura ≥80% total e ≥85% nos módulos novos; `ruff` limpo.
- TDD obrigatório (escrever teste antes da implementação).

Prior art: testes existentes em `tests/test_extractors/`, `tests/test_engine/`,
`tests/test_validators/`, `tests/test_report/`; fixtures em `tests/fixtures/`
(há `parquet/arrecadacao_fixture.parquet`).

## Out of Scope

- **Reconciliação dos ~5%** de ICMS (SIGDEF 11.494 vs golden 10.917) — fica para
  revisão metodológica formal; até lá o golden segue ancorado em GFIS2.
- **Migração da ingestão IMESC** de transcrição manual para fonte máquina
  (xlsx/API `imesc.ma.gov.br`).
- **Investigar o VRR 2019 suspeito** (=0,213).
- **Uso do paper CGE** (`63_...pdf`) como fonte de dados — ele não tem VAB/PIB/
  ICMS utilizável para MA; no máximo referência metodológica.
- **Mudança de alíquota mid-year** (já era limitação conhecida).
- Decomposição setorial do gap (o ICMS por setor do SIGDEF e o VAB por setor do
  IMESC permitem isso no futuro, mas não agora).

## Further Notes

- A correção do offset do SIGDEF vindica o arquivo: o "16×" era artefato de
  parsing, não problema do dado. O dado é nominal e bate com a realidade nacional.
- A renúncia AMF tem fonte `BI-Oracle-SEFAZ-MA` — o mesmo Oracle que o CLAUDE.md
  aponta como fonte correta (mas inacessível) de importações Siscomex; pode ser
  um caminho futuro de acesso.
- Os 3 arquivos AMF têm cobertura sobreposta (anos repetidos entre vintages); o
  `AmfRenunciaReader` deve escolher a vintage mais apropriada por ano e logar a
  escolha.
- Pipeline alvo permanece de 7 estágios: CLI Parse → Config → Extract → Validate
  → Calculate → Report → Output. A decomposição entra entre Calculate e Report.
