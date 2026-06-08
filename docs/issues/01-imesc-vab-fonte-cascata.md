# #1 — IMESC VAB como fonte nº1 da cascata

> Tipo: AFK · Label: ready-for-agent · Parent: `prd.md`

## What to build

Adicionar o relatório trimestral do PIB do MA (IMESC) como a fonte primária de
VAB do pipeline, eliminando a aproximação `VAB = PIB × 0,8932` e o lag de ~2 anos
do IBGE para os anos cobertos (2021–2025).

Fluxo end-to-end: dado transcrito da **Tabela 15 (valores correntes)** do
relatório IMESC → `ImescPibExtractor` → topo da cascata de VAB em
`config/fontes.yaml` (novo) → cálculo VRR → saída. Para período **anual**, o VAB
é a soma dos 4 trimestres; para período **trimestral**, é o valor do trimestre
direto (sem rateio). Anos não cobertos (<2021) caem para o SIDRA (fallback).

Usar **somente** a Tabela 15 (correntes/nominal). A Tabela 16 (preços de 2010) é
proibida no cálculo — misturaria valores reais com ICMS nominal e distorceria o
VRR. Coluna a usar: **VAB** (`PIB = VAB + Impostos/IPLS`).

## Acceptance criteria

- [ ] `ImescPibExtractor` lê os dados transcritos da Tabela 15 e retorna VAB por período.
- [ ] `--periodo 2024` (antes impossível por lag) calcula com VAB real do IMESC.
- [ ] `--periodo 2022` produz VAB = 124.859 (soma dos 4 trimestres) → **golden 2022 segue verde** (VRR 0,518 ± 0,002).
- [ ] `--periodo 2022-T1` retorna VAB = 25.871 (trimestre direto, sem rateio).
- [ ] `config/fontes.yaml` define a cascata de VAB: IMESC → SIDRA → IPEADATA → BCB Focus → AutoARIMA → `--vab-manual`.
- [ ] Ano <2021 cai corretamente para o fallback SIDRA.
- [ ] Testes de `ImescPibExtractor` (anual=Σ4tri; trimestral direto; ano fora de cobertura sinaliza fallback). Cobertura ≥85% no módulo novo.
- [ ] `pytest` verde, `ruff check src/` limpo.
- [ ] `MotorVRR` (`engine/vrr.py`) inalterado.

## Blocked by

None - can start immediately.
