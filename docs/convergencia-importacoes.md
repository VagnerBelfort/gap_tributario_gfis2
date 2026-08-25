# Convergência das importações do MA — Siscomex × MDIC

**Escopo:** 2019 a 2025
**Data:** 10 de agosto de 2026 — **revisado em 24 de agosto de 2026**, após a
SEFAZ-MA esclarecer a origem do número apresentado no 79º ENCAT (item 1.1) e
confirmar o tratamento do campo `TDS_SITUACAO` (item 3.2).

---

## 1. Resultado

Duas apurações independentes das importações do Maranhão, construídas a partir
de fontes diferentes e por equipes diferentes, convergem:

| Apuração | Fonte | Conceito | 2022 (R$ bi) |
|---|---|---|---:|
| **Nossa** | Siscomex (Oracle SEFAZ) | CIF, domicílio fiscal do importador | **39,70** |
| Reconversão nossa do dado da SEFAZ | MDIC ComexStat | FOB em US$ × PTAX venda, UF do produto | 38,79 |
| SEFAZ-MA (NEC), planilha original | MDIC ComexStat | FOB em US$ × PTAX compra, UF do produto | 38,78 |

Diferença de **+2,4%**, no sentido esperado: o valor aduaneiro inclui frete e
seguro, que o FOB não inclui. A convergência se mantém em toda a série
2019–2025 (item 2).

O Gap Tributário de 2022 resultante da nossa apuração:

| | Valor |
|---|---:|
| Importações (R$ milhões) | 39.704 |
| VAT_VRR | 0,473 |
| Gap (R$ milhões) | 12.792 |

**Nota sobre a arrecadação.** O VAT_VRR de 0,473 usa o ICMS do SIGDEF
(R$ 11.495 milhões). Com o GFIS2 (R$ 10.917 milhões), o VAT_VRR seria 0,450 e
o gap, R$ 13.369 milhões. A divergência entre as duas fontes de arrecadação
está registrada no item 5 e é independente da discussão sobre importações.

### 1.1 Sobre os R$ 21,9 bilhões do 79º ENCAT

A apresentação do 79º ENCAT registra importações de R$ 21.924 milhões para
2022, e por algum tempo perseguimos a definição que reproduzisse esse valor.
Em 24/08/2026 o autor da apuração (Jomar Fernandes Pereira, SEFAZ-MA) informou
por mensagem de voz que o número **não é uma apuração**, e sim uma referência
usada para a apresentação; que o dado do sistema deles para 2022 é
R$ 38,7 bilhões; e que a fonte recomendada é o MDIC. Transcrição integral e
capturas de tela em `resposta_jomar/transcricao.md`. O valor da planilha deles,
R$ 38.780.445.567,61, sai de US$ 7,509 bi × 5,1648 (PTAX compra); com a ponta
de venda que o pipeline usa, R$ 38,79 bi.

Não há, portanto, memória de cálculo a reproduzir. As tentativas de localizar a
definição equivalente ficaram sem objeto e foram removidas deste documento.

---

## 2. As duas medições

### 2.1 Nossa apuração — Siscomex

Os dados vêm do schema `APL_SISCOMEX` no Oracle C3 da SEFAZ, tabelas
`TAB_DECL_SISCOMEX` (declarações) e `TAB_ITEM_SISCOMEX` (itens). O acesso só
existe de dentro da rede da SEFAZ, então a extração roda no cluster Cloudera
via PySpark (`scripts/snapshot_siscomex.py`).

O campo decisivo é `TDS_UF_IMPORTADOR`, o domicílio fiscal do importador
declarado na DI. É ele que distingue a importação maranhense da carga que
apenas passa pelo Porto de Itaqui — distinção que o dado público do MDIC não
permite fazer. O valor é o aduaneiro (CIF), aderente à base de cálculo do ICMS
na importação, que compreende frete e seguro.

**Proteção de sigilo fiscal:** CNPJ e nome do importador são usados apenas
dentro do cluster, para classificar cada declaração. O arquivo exportado é
agregado por ano, trimestre, capítulo NCM, UF de despacho e UF do importador.
Nenhum dado individualizado sai do ambiente da SEFAZ.

### 2.2 Apuração da SEFAZ — MDIC ComexStat

Valor US$ FOB de importação por UF do produto, convertido pela PTAX média do
ano. Reprodutível por comando, sem acesso à rede da SEFAZ:

```bash
# US$ FOB de importação por UF e ano
curl -s -X POST 'https://api-comexstat.mdic.gov.br/general' \
  -H 'Content-Type: application/json' \
  -d '{"flow":"import","monthDetail":false,"yearDetail":true,
       "period":{"from":"2019-01","to":"2025-12"},
       "filters":[],"details":["state"],"metrics":["metricFOB"]}'

# PTAX diária do ano, para a média
curl -s "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/\
CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)\
?@dataInicial='01-01-2022'&@dataFinalCotacao='12-31-2022'&\$format=json"
```

A série completa se refaz por comando, sem rede da SEFAZ:

```bash
uv run python scripts/serie_mdic_ptax.py            # tabela do §3, das duas APIs
uv run python scripts/serie_mdic_ptax.py --de 2011 --ate 2025
```

**Câmbio.** Usamos a média das cotações de **venda** do ano — 5,1655 para 2022 —,
a mesma ponta que o pipeline aplica em todas as conversões. A SEFAZ informou
5,1648, que é a média de compra; as duas diferem em 0,01% e não alteram
nenhuma conclusão.

---

## 3. Convergência 2019–2025

| Ano | MDIC US$ bi FOB | PTAX média | MDIC R$ bi | Siscomex R$ bi (domicílio) | Δ vs MDIC | Siscomex R$ bi (despacho) | Trânsito |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2019 | 3,552 | 3,9461 | 14,02 | 14,97 | +6,8% | 15,76 | +5,2% |
| 2020 | 1,977 | 5,1578 | 10,20 | 11,50 | +12,8% | 12,58 | +9,3% |
| 2021 | 4,182 | 5,3956 | 22,57 | 24,10 | +6,8% | 25,36 | +5,2% |
| 2022 | 7,509 | 5,1655 | 38,79 | 39,70 | +2,4% | 43,55 | +9,7% |
| 2023 | 4,859 | 4,9953 | 24,27 | 25,65 | +5,7% | 26,42 | +3,0% |
| 2024 | 3,978 | 5,3920 | 21,45 | 23,34 | +8,8% | 23,38 | +0,1% |
| 2025 | 4,758 | 5,5859 | 26,58 | 27,21 | +2,4% | 27,99 | +2,9% |

A coluna **Δ vs MDIC** é positiva nos sete anos, entre +2,4% e +12,8%, mediana
+6,8%.

O relatório aplica isso como controle a cada execução (`engine/comparacao.py`):
desvio fora do corredor sai como aviso no log e como sinalização no PDF e no
Excel. São duas grandezas distintas, e o código as separa:

- **Faixa observada** (`FAIXA_OBSERVADA`): +2,4% a +12,8%, o que foi medido. É o
  que o relatório exibe como referência de leitura, e o que
  `scripts/serie_mdic_ptax.py` recalcula das fontes.
- **Corredor de controle** (`CORREDOR_CONTROLE`): +2% a +13%, deliberadamente
  mais largo. O extremo inferior medido, 2022, é +2,368% — colar o controle no
  valor exibido reprovaria justamente o ano que o originou.

O controle só se aplica a períodos **anuais**: a faixa foi medida em totais de
ano, e num trimestre o desvio é calculado mas não julgado. É a assinatura esperada de CIF sobre FOB, e é o que sustenta as duas
apurações como medições coerentes do mesmo fenômeno — não um encontro fortuito
em 2022.

A coluna **Trânsito** mede quanto o valor desembaraçado no Maranhão excede o
valor de importadores maranhenses. Fica entre 0,1% e 9,7%: é a carga que passa
por Itaqui a caminho de outros estados, e que a apuração por domicílio fiscal
corretamente exclui.

Note que o MDIC fica **abaixo** da nossa apuração em todos os anos, apesar de
incluir o trânsito. O desconto do FOB frente ao CIF é maior que o trânsito, e
as duas distorções operam em sentidos opostos.

---

## 4. Como validamos

Cada regra de apuração foi testada contra evidência independente, e três
premissas que pareciam razoáveis foram derrubadas no processo.

### 4.1 Teste de impossibilidade aritmética

A primeira apuração indicou ICMS devido nas importações de 2022 no valor de
R$ 13,1 bilhões. O ICMS **total** arrecadado pelo Maranhão naquele ano foi de
R$ 10,9 bilhões. Como a parte não pode exceder o todo, o número estava errado.

A causa: a tabela de declarações guarda uma linha por **versão** da DI — cada
retificação gera um novo registro. Somar todas as versões multiplica os
valores. São 57.574 linhas para 44.847 declarações distintas. Corrigido isso, e
usando a chave composta correta (`NUM_DECL` + `SEQ_DECL`) no cruzamento com os
itens, o valor caiu para patamar coerente.

### 4.2 Situação da DI (`TDS_SITUACAO`)

O campo assume os valores `S` e `N`. A leitura intuitiva seria tratar `N` como
declaração cancelada e descartá-la — em 2022, isso removeria R$ 12,5 bilhões.

A evidência empírica já apontava contra: nenhuma das declarações `N` está sem
data de desembaraço, elas recolhem Imposto de Importação e IPI em proporção
comparável às `S` (39% contra 49%), e 3.807 das 5.079 permanecem como última
versão de sua declaração. Uma DI cancelada não desembaraça e não recolhe
tributo.

A SEFAZ-MA confirmou formalmente (Alan Carlos de Moura Lima, TI, 24/08/2026),
após análise das procedures do banco:

> "esse status não tem nenhum impacto nos nossos dados. Ele vem diretamente da
> Receita, mas não é utilizado aqui. Pode ser desconsiderado."

Todas as DIs desembaraçadas entram. A decisão está registrada na proveniência
do relatório gerado pelo sistema.

### 4.3 Atribuição por cadastro, com grupo de controle duplo

Parte das declarações não traz a UF do importador preenchida. Resolvemos essas
pelo cruzamento do CNPJ com o cadastro de contribuintes da SEFAZ
(`b_contribuinte`, campo `uf_icms`).

Antes de confiar no método, testamos contra dois grupos de controle:

- **Declarações que o Siscomex já marca como MA:** dos 797 CNPJs, 673
  encontraram correspondência no cadastro, e **todos** retornaram MA. Nenhum
  falso positivo.
- **Declarações que o Siscomex marca como de outra UF:** dos 855 CNPJs, 130
  encontraram correspondência, e o cadastro devolveu a UF correta na maioria —
  MG, SP, AM, GO, BA, entre outras. Em 12 casos (≈9%) o cadastro indicou MA
  onde o Siscomex indicava outra UF.

O segundo teste é o que importa: se o cadastro devolvesse "MA" para todos, ele
estaria apenas registrando inscrição estadual no Maranhão, e não domicílio
fiscal. Como devolve a UF real, o método é válido — com margem de erro
conhecida de cerca de 9%, provavelmente casos de substituto tributário sediado
fora do estado.

O procedimento recuperou 2.812 declarações maranhenses, no valor de R$ 7,71
bilhões, que estavam fora da conta. Restaram 30 declarações sem atribuição,
somando R$ 9,1 milhões — 0,02% do total.

### 4.4 Medição do trânsito pelo Porto de Itaqui

A premissa até então adotada era que os capítulos NCM 27 (combustíveis) e 31
(fertilizantes) seriam quase integralmente carga em trânsito para outros
estados, e por isso deveriam ser excluídos das importações do Maranhão.

Com o Siscomex foi possível medir em vez de presumir. Para 2022, considerando
apenas as declarações desembaraçadas no Maranhão:

| Capítulo | Importador do MA | Importador de outra UF | Trânsito |
|---|---:|---:|---:|
| 27 — combustíveis | R$ 26,45 bi | R$ 2,89 bi | **9,8%** |
| 31 — fertilizantes | R$ 7,87 bi | R$ 1,73 bi | **15,4%** |

Os dois capítulos de fato dominam o movimento de Itaqui, respondendo por cerca
de 92% do valor desembaraçado. Mas dominância não é trânsito: 90% do capítulo
27 e 70% do capítulo 31 pertencem a importadores maranhenses.

### 4.5 Auditoria das exportações

Como a exportação entra na fórmula com sinal negativo, um erro nela desloca o
resultado tanto quanto um erro na importação. Verificamos se o mesmo problema
de trânsito a afetava.

Não afeta. O ComexStat utiliza critérios distintos para os dois fluxos: nas
exportações, a UF é a do **estado produtor**; nas importações, a de origem ou
destino declarada. A verificação de magnitude confirma: o Maranhão exportou
US$ 5,74 bilhões em 2022, enquanto apenas o minério de ferro de Carajás
escoado por Ponta da Madeira supera US$ 15 bilhões — volume atribuído ao Pará,
como deveria ser.

---

## 5. Limitações declaradas

**Assimetria entre CIF e FOB.** As importações são apuradas em valor aduaneiro,
que inclui frete e seguro; as exportações, em valor FOB, que não os inclui.
Isso amplia a perna de importação da base em cerca de 10% a 15%. O ComexStat
não publica valor CIF por unidade da federação, o que impede a correção pelo
dado público. A razão limpa entre os dois conceitos pode ser extraída do
próprio Siscomex, que traz valor da mercadoria e valor aduaneiro na mesma DI.

**Margem da atribuição por cadastro.** Conforme o item 4.3, inscrição estadual
no Maranhão não equivale a domicílio fiscal no estado. O viés identificado é de
aproximadamente 9% e atua no sentido de ampliar o total atribuído ao MA.

**Divergência entre fontes de arrecadação.** SIGDEF e GFIS2 apresentam valores
distintos para o ICMS arrecadado em 2022 — R$ 11.495 milhões e R$ 10.917
milhões, respectivamente, diferença de 5%. Como a arrecadação é o numerador do
VAT_VRR, essa divergência precisa ser reconciliada antes da consolidação de
séries históricas. A cobertura do SIGDEF encerra-se em 2023, o que introduz
mudança de fonte entre 2023 e 2024.

**Cobertura temporal.** A análise cobre 2019 em diante, limite imposto pela
arrecadação confiável. O Siscomex apresenta cobertura confiável a partir de
2013 — a série do MDIC confirma esse limite por fonte externa: em 2011 e 2012 a
apuração pelo Siscomex fica 76% e 44% **abaixo** do MDIC, contra o corredor de
+2% a +13% observado de 2013 em diante.

**Importações interestaduais.** A metodologia do NEEF relaciona, entre suas
fontes, as importações originadas de outras unidades da federação. A
implementação atual contempla apenas as importações do exterior. Caso a base
pretendida inclua as entradas interestaduais, há um termo a ser incorporado à
fórmula — questão que merece deliberação metodológica.
