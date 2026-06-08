# #5 — Proveniência nas saídas

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Tornar cada resultado rastreável até sua fonte. Criar a dataclass `Proveniencia`
(origem, fonte, data de extração, observações) em `models.py`, propagá-la pelas
fontes novas (IMESC, SIGDEF, AMF) e renderizar um bloco de proveniência no
relatório PDF e Excel.

## Acceptance criteria

- [ ] `Proveniencia` definida em `models.py`.
- [ ] VAB (IMESC), ICMS (SIGDEF) e renúncia (AMF) carregam sua `Proveniencia`.
- [ ] PDF e Excel mostram, por variável, a origem, a fonte e a data de extração.
- [ ] Caveats de cobertura (VAB ≥2021; renúncia prospectiva; alíquota 18→20% em 2023) aparecem no relatório.
- [ ] Testes cobrindo a propagação da proveniência; cobertura mantida.
- [ ] `pytest` verde, `ruff` limpo, golden inalterado.

## Blocked by

- #1 — IMESC VAB como fonte nº1 da cascata
- #3 — SIGDEF como fonte de ICMS
- #4 — Decomposição do gap
