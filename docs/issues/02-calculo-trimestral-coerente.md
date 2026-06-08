# #2 — Cálculo trimestral coerente (Exp/Imp trimestralizados)

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Completar o caminho trimestral do VRR. Com o VAB já trimestral nativo (vindo do
#1), agregar **Exportações e Importações** do ComEx (mensal) para o trimestre,
de modo que `--periodo YYYY-TN` calcule a fórmula inteira com dado real, sem
nenhuma premissa de rateio.

`Base_Tn = VAB_Tn − Exp_Tn + Imp_Tn`, com cada componente do próprio trimestre.

## Acceptance criteria

- [x] Exp/Imp são agregados mês→trimestre para períodos trimestrais. *(já em `comex.py:131-134`, `_TRIMESTRE_MESES`)*
- [x] `--periodo 2024-T2` calcula VRR/gap ponta a ponta com VAB, Exp e Imp do trimestre. *(test `test_pipeline_trimestral_t1_usa_exp_imp_do_trimestre`)*
- [x] Período anual permanece inalterado (soma/uso anual). *(test `test_pipeline_trimestral_difere_do_anual`)*
- [x] Nenhum rateio por peso de ICMS é usado em lugar nenhum. *(grep confirmou ausência; VAB trimestral é nativo do IMESC; sem `engine/seasonality.py`)*
- [x] Testes do caminho trimestral; cobertura ≥85% no que for novo. *(`comex.py` 100%; +2 testes de integração)*
- [x] `pytest` verde (301), `ruff` limpo, golden inalterado (VRR≈0,52).

## Blocked by

- #1 — IMESC VAB como fonte nº1 da cascata
