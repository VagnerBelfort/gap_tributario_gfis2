# #4 — Decomposição do gap (policy vs compliance)

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Separar o gap total em **policy gap** (renúncia fiscal legal) e **compliance
gap** (evasão), padrão RA-GAP do FMI, e mostrar no relatório.

- `AmfRenunciaReader` (transform puro): mescla os 3 xlsx da AMF Tabela 7 (LDO
  2022/2025/2026, cobertura combinada 2022–2029), seleciona o ano (escolhendo a
  vintage apropriada e logando a escolha) e soma as modalidades de **ICMS**
  (Crédito Presumido + Isenção + Redução de Base de Cálculo); IPVA fora.
- `GapDecomposer` (`engine/gap_decomposition.py`, função pura): recebe
  `ResultadoGap` + renúncia do ano e retorna `policy_gap = renúncia`,
  `compliance_gap = gap_absoluto − policy_gap`, mais percentuais.
- Novas linhas no PDF e no Excel com a decomposição (e detalhamento por modalidade).

`MotorVRR` permanece **intocado** — a decomposição é estágio posterior, aditivo,
entre Calculate e Report.

## Acceptance criteria

- [ ] `AmfRenunciaReader` retorna renúncia ICMS por ano (2022 → 2.182 mi) e detalhe por modalidade.
- [ ] `GapDecomposer` decompõe o golden 2022: Gap 10.148 = Policy 2.182 + Compliance 7.966.
- [ ] Ano sem renúncia → degrada para "só gap total" (sem quebrar).
- [ ] Renúncia > gap → tratada/avisada (compliance negativo).
- [ ] PDF e Excel exibem as linhas de decomposição.
- [ ] Caveat no relatório: renúncia é estimativa LDO (prospectiva).
- [ ] Testes de `AmfRenunciaReader` e `GapDecomposer`; cobertura ≥85% nos módulos novos.
- [ ] `pytest` verde, `ruff` limpo, golden inalterado.

## Blocked by

None - can start immediately p/ 2022 (valor pleno em anos recentes depende de #1).
