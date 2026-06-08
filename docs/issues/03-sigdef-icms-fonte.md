# #3 — SIGDEF como fonte de ICMS (fix de offset)

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Adotar o arquivo SIGDEF (`docs/20260204_por-setor.xls`) como fonte primária de
ICMS arrecadado, corrigindo o desalinhamento de colunas do export.

- `SigdefParser` (transform puro): lê o `.xls` e aplica o **fix de offset +1**,
  lendo o ICMS total pela **coluna de posição 21** (rotulada erroneamente
  `va_icms_outras`); ignora os rótulos do export. Valida sanidade (soma nacional
  ~ ICMS Brasil real) e materializa um **parquet limpo** (não reparsear o `.xls`
  a cada run).
- `SigdefIcmsExtractor`: lê o parquet limpo, agrega mês→período, retorna ICMS.
- Registrar como fonte primária de ICMS em `config/fontes.yaml` (SIGDEF → GFIS2).

**Pinagem do golden:** o teste golden 2022 permanece alimentado pelo GFIS2 (o
SIGDEF daria 11.494 vs golden 10.917, ~5%, reconciliação fora de escopo). O
SIGDEF é primário para todos os demais períodos.

## Acceptance criteria

- [ ] `SigdefParser` produz parquet limpo com ICMS total correto (MA 2022 ≈ 11.494 mi; nacional 2022 ≈ 691 bi).
- [ ] Leitura é por **posição** (col. 21), não por rótulo.
- [ ] `SigdefIcmsExtractor` retorna ICMS por período a partir do parquet limpo.
- [ ] Golden 2022 continua ancorado em GFIS2 e **verde**.
- [ ] Testes de `SigdefParser` com fixture pequena reproduzindo o offset; cobertura ≥85%.
- [ ] `pytest` verde, `ruff` limpo, `MotorVRR` intocado.

## Blocked by

None - can start immediately (paralelo ao #1).
