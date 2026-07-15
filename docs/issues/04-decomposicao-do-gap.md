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

- [x] `AmfRenunciaReader` retorna renúncia ICMS por ano (2022 → 2.182,13 mi) e detalhe por modalidade (Créd.Presumido 1.268 + Isenção 520 + Redução BC 394). Cobertura 2022–2029 (vintages LDO-2022/2025/2026).
- [x] `decompor_gap` decompõe o golden 2022: Gap 10.148,22 = Policy 2.182,13 (21,50%) + Compliance 7.966,09 (78,50%).
- [x] Ano sem renúncia → reader devolve `None`; cli degrada para "só gap total" (sem quebrar). Trimestral também omite (renúncia é anual).
- [x] Renúncia > gap → `compliance_negativo=True` + aviso no PDF/Excel.
- [x] PDF e Excel exibem a seção "1.1 Decomposição do Gap" + detalhe por modalidade.
- [x] Caveat no relatório: renúncia é estimativa prospectiva da LDO (vintage citada).
- [x] Testes de `AmfRenunciaParser/Reader` e `decompor_gap`; cobertura **100%** nos módulos novos.
- [x] `pytest` verde (299 passed), `ruff` limpo, golden inalterado, `MotorVRR` intocado.

## Blocked by

None - can start immediately p/ 2022 (valor pleno em anos recentes depende de #1).
