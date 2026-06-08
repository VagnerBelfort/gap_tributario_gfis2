# #5 — Proveniência nas saídas

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Tornar cada resultado rastreável até sua fonte. Criar a dataclass `Proveniencia`
(origem, fonte, data de extração, observações) em `models.py`, propagá-la pelas
fontes novas (IMESC, SIGDEF, AMF) e renderizar um bloco de proveniência no
relatório PDF e Excel.

## Acceptance criteria

- [x] `Proveniencia` definida em `models.py`. *(frozen dataclass: variavel/origem/fonte/data_extracao/observacoes)*
- [x] VAB (IMESC), ICMS (SIGDEF) e renúncia (AMF) carregam sua `Proveniencia`. *(método `proveniencia(...)` em cada extrator)*
- [x] PDF e Excel mostram, por variável, a origem, a fonte e a data de extração. *(seção "4. Proveniência das Fontes")*
- [x] Caveats de cobertura (VAB ≥2021; renúncia prospectiva; alíquota 18→20% em 2023) aparecem no relatório. *(`_CAVEATS_COBERTURA`)*
- [x] Testes cobrindo a propagação da proveniência; cobertura mantida. *(+12 testes; total 92%; extratores 100%, models 99%, reports 93%)*
- [x] `pytest` verde (311), `ruff` limpo (src/), golden inalterado (VRR≈0,52).

## Blocked by

- #1 — IMESC VAB como fonte nº1 da cascata
- #3 — SIGDEF como fonte de ICMS
- #4 — Decomposição do gap
