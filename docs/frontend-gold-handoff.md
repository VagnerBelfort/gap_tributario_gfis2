# Repasse para o front-end — tela Gap Tributário

**Para:** dev que constrói a tela `Gap Tributário › Visão Geral`
**Design de referência:** `design/project/Gap Tributario GFIS2 v2.dc.html`
**Arquitetura completa:** `docs/pipeline-impala-medallion.md` (as queries estão no §4)

---

## Comece por aqui

**As tabelas já existem e já estão populadas em `gfis2_ouro`** (desde 2026-07-29,
mesmos dados que já estavam em `gfis2_dev`). Nada a rodar — é só consultar:

```bash
impala-shell -i sefazbige02.sefaz.ma.gov.br -d default -k --ssl \
  --ca_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem \
  -q "SELECT * FROM gfis2_ouro.g_gap_resultado WHERE tipo_periodo='A' ORDER BY ano;"
```

| Tabela | Linhas | Serve |
|---|---|---|
| `gfis2_ouro.g_gap_resultado` | 6 | KPIs, evolução, waterfall, gráfico trimestral, tabela de anos, tabela trimestral |
| `gfis2_ouro.g_gap_decomposicao` | 24 | Policy × Compliance + modalidades de renúncia |
| `gfis2_ouro.g_gap_proveniencia` | 42 | Tabela "Componentes da fórmula & proveniência" |

`gfis2_dev` mantém as mesmas 3 tabelas para uso em desenvolvimento/testes.

**Queries prontas: `docs/pipeline-impala-medallion.md` §4.**

Hoje só há linhas anuais, de 2020 a 2025. Para repopular, siga
`docs/runbook-carga-impala.md`: o CLI calcula, a carga passa pelo `gfis2_dev` e
chega à `gfis2_ouro` por cópia.

Se as tabelas sumirem do Impala após uma recriação, invalide **tabela a tabela**
(`INVALIDATE METADATA` recebe tabela, não database):

```sql
INVALIDATE METADATA gfis2_ouro.g_gap_resultado;
INVALIDATE METADATA gfis2_ouro.g_gap_decomposicao;
INVALIDATE METADATA gfis2_ouro.g_gap_proveniencia;
```

---

## Os valores são reais. O schema não muda.

Os números são a saída do CLI (`python -m gap_tributario --exportar-ouro`), com
importações do Siscomex (DI + DUIMP) e ICMS do GFIS2. Cada carga marca as linhas
com `id_execucao = 'CARGA_CLI_<data do cálculo>'`; a de 31/08/2026 era
`CARGA_MANUAL_2026-08-31`.

Uma carga nova muda só valores: tabela, colunas, tipos, grão e os valores
categóricos (`fonte_*`, `variavel`, `componente`) ficam iguais. O job de carga
aborta se algum deles divergir. A separação DI × DUIMP aparece no texto de
`observacoes` da linha "Importações" da proveniência.

---

## Casos de borda

Estes são os que quebram a tela se não forem tratados. Os casos 2 e 3 não têm
dado na carga atual, mas voltam quando 2026 entrar:

**1. Ano sem decomposição (2019, 2020, 2021)**
A AMF Tabela 7 só cobre de 2022 em diante. Esses anos **não têm linha** em
`g_gap_decomposicao` — o resultado vem vazio, não vem zerado. O protótipo já resolve
com a mensagem: *"Sem renúncia fiscal disponível para {ano} — a cobertura da AMF
Tabela 7 (LDO/MA) inicia em 2022."*

**2. Ano parcial (2026)**
Só tem **T1 e T2**. `flg_parcial = TRUE`. A tela precisa: desabilitar os chips T3/T4,
desenhar "sem dados" nos slots vazios do gráfico trimestral, mostrar o badge
*"Dados parciais — T1–T2/2026"*, e **não** oferecer decomposição (renúncia é anual;
ano parcial não tem).

**3. Ano estimado (2024, 2025, 2026)**
`flg_estimado = TRUE` — o VAB veio do fallback porque o IBGE publica com ~2 anos de
lag. A tela marca com `†` e mostra o badge de estimativa.

**4. Período trimestral não tem decomposição — nunca**
Independente do ano. A renúncia da AMF é anual por natureza. Em modo trimestral,
mostre: *"Decomposição indisponível para períodos trimestrais."*

**5. `compliance_gap` pode ser negativo**
Quando a renúncia estimada excede o gap total. Não acontece na carga atual, mas o
campo `flg_compliance_negativo` existe e pode vir `TRUE` com dado real — a barra
empilhada precisa aguentar isso sem estourar o layout.

---

## Contratos que valem a pena conhecer

**Anual e trimestral moram na mesma tabela.** Alterna com `WHERE`, não com outra query:

```sql
-- anual
WHERE ano = 2022 AND tipo_periodo = 'A' AND nro_trimestre = 0
-- trimestral
WHERE ano = 2022 AND tipo_periodo = 'T' AND nro_trimestre = 3
```

**A ouro guarda número; formatação é sua.** `R$ 21.065 mi`, `0,518`, `57,4%` são
decisões de UI. O protótipo já tem `fmt()`, `fPct()` e `fVrr()` — reaproveite.

**Os deltas ("▲ +3,2% vs 2024") não vêm do banco.** São calculados na tela, como o
protótipo faz. Cuidado com a regra: o anterior de **T1 é o T4 do ano anterior**, não
o T4 do mesmo ano.

**`vrr` vem como ratio 0–1**, não percentual. `gap_percentual` já vem × 100.

**A ordem da decomposição não é alfabética.** Use o `ORDER BY CASE` do §4:
GAP_TOTAL → POLICY → COMPLIANCE → modalidades.

---

## O que NÃO fazer

- **Não consulte `gfis2_prata` nem `gfis2_bronze`.** A tela lê só `g_gap_*`. A prata
  guarda candidatos de fonte que ainda não foram eleitos — números que perderam a
  cascata. Ler de lá mostra dado errado com cara de certo.
- **Não escreva em `gfis2_ouro.g_arrecadacao`.** É a arrecadação oficial, de outro
  time. Este projeto só lê.
- **Não derive o gap na tela.** `gap_absoluto` e `vrr` vêm calculados pelo motor
  aprovado, sob trava metodológica. Recalcular no front-end cria um segundo lugar
  onde a fórmula existe — e um dia os dois vão discordar.
