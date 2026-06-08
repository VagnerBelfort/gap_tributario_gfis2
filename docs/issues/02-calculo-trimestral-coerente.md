# #2 — Cálculo trimestral coerente (Exp/Imp trimestralizados)

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Completar o caminho trimestral do VRR. Com o VAB já trimestral nativo (vindo do
#1), agregar **Exportações e Importações** do ComEx (mensal) para o trimestre,
de modo que `--periodo YYYY-TN` calcule a fórmula inteira com dado real, sem
nenhuma premissa de rateio.

`Base_Tn = VAB_Tn − Exp_Tn + Imp_Tn`, com cada componente do próprio trimestre.

## Acceptance criteria

- [ ] Exp/Imp são agregados mês→trimestre para períodos trimestrais.
- [ ] `--periodo 2024-T2` calcula VRR/gap ponta a ponta com VAB, Exp e Imp do trimestre.
- [ ] Período anual permanece inalterado (soma/uso anual).
- [ ] Nenhum rateio por peso de ICMS é usado em lugar nenhum.
- [ ] Testes do caminho trimestral; cobertura ≥85% no que for novo.
- [ ] `pytest` verde, `ruff` limpo, golden inalterado.

## Blocked by

- #1 — IMESC VAB como fonte nº1 da cascata
