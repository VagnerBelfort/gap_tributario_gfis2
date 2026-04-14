## Overview

O Gap Tributário do ICMS representa a diferença entre o ICMS que deveria ser arrecadado (potencial) e o ICMS efetivamente arrecadado pelo estado. A **Calculadora de Gap Tributário** permite à Sefaz do Maranhão calcular esse gap para períodos específicos, aplicando a metodologia ESAF-2012 combinada com o VRR (VAT Revenue Ratio) da OCDE, adaptada à realidade maranhense.

O produto resolve a falta de visibilidade sobre a eficiência da arrecadação do ICMS no estado, fornecendo métricas objetivas para apoiar a tomada de decisão de analistas fiscais e gestores. Internacionalmente, países da União Europeia medem gaps médios de ~9,5% e os EUA reportam compliance de ~85% — o Maranhão precisa de ferramenta equivalente para mensurar e acompanhar seu desempenho tributário.

O sistema consolida dados de três fontes oficiais — importações (Siscomex), arrecadação estadual (GFIS2) e comércio exterior (MDIC ComEx) — e entrega resultados em relatórios exportáveis (PDF/Excel) com os componentes do cálculo.

## Goals

- **Mensurar o gap tributário**: Permitir o cálculo do gap do ICMS do Maranhão para períodos trimestrais ou anuais, utilizando a metodologia ESAF-2012/VRR da OCDE
- **Apoiar a tomada de decisão**: Fornecer a analistas fiscais e gestores os dados necessários para direcionar políticas de fiscalização e arrecadação
- **Gerar relatórios formais**: Produzir relatórios exportáveis (PDF/Excel) com os componentes do cálculo (ICMS potencial, ICMS arrecadado, VRR e gap), adequados para uso institucional
- **Consolidar fontes de dados**: Unificar dados de 3 fontes oficiais (importações, arrecadação e comércio exterior) em uma visão integrada do gap
- **Validação cruzada**: Permitir comparação dos resultados calculados com benchmarks internacionais (gap médio EU ~9,5%) e estimativas anteriores da Sefaz
- **Adoção pelos usuários**: Ser utilizado por analistas e gestores como ferramenta regular de apoio às suas decisões sobre arrecadação e fiscalização

## User Stories

- **Como analista fiscal**, quero calcular o gap tributário do ICMS para um trimestre específico, para identificar a magnitude da perda de arrecadação e embasar análises técnicas
- **Como gestor da Sefaz**, quero visualizar o valor consolidado do gap tributário do estado, para tomar decisões estratégicas sobre fiscalização e política tributária
- **Como analista fiscal**, quero exportar o relatório do gap em PDF ou Excel, para compartilhar os resultados com outras áreas e usar em apresentações
- **Como gestor da Sefaz**, quero ver os componentes intermediários do cálculo (ICMS potencial, ICMS arrecadado, VRR), para entender como o gap é composto e validar os resultados
- **Como analista fiscal**, quero selecionar o período de análise (trimestral ou anual), para calcular o gap na granularidade mais adequada para cada necessidade
- **Como gestor da Sefaz**, quero contextualizar o gap calculado com benchmarks internacionais, para avaliar o desempenho do Maranhão no cenário global

## Core Features

### 1. Cálculo do Gap Tributário

Permite ao usuário calcular o gap tributário do ICMS do Maranhão aplicando a metodologia ESAF-2012 combinada com o VRR da OCDE. O cálculo consolida dados de três fontes oficiais para estimar o ICMS potencial e compará-lo com o efetivamente arrecadado.

**Por que é importante**: É a funcionalidade central do produto — sem ela, não há como mensurar a diferença entre o potencial e o efetivo da arrecadação do ICMS.

**Requisitos funcionais**:
1. O sistema deve calcular o ICMS potencial estimado do estado com base nos dados das fontes oficiais
2. O sistema deve utilizar o ICMS efetivamente arrecadado consolidado do período
3. O sistema deve calcular o VRR (VAT Revenue Ratio) conforme metodologia da OCDE adaptada ao Maranhão
4. O sistema deve calcular o valor do gap tributário (diferença entre potencial e arrecadado)
5. O sistema deve apresentar o gap como valor absoluto (R$) e como percentual do ICMS potencial

### 2. Seleção de Período

Permite ao usuário escolher o período para o qual deseja calcular o gap tributário, com opções de granularidade trimestral ou anual.

**Por que é importante**: Diferentes análises requerem diferentes horizontes temporais — a análise trimestral permite acompanhamento mais frequente, enquanto a anual oferece visão consolidada.

**Requisitos funcionais**:
1. O usuário deve poder selecionar um período trimestral (Q1, Q2, Q3, Q4 de um ano específico)
2. O usuário deve poder selecionar um período anual completo
3. O sistema deve validar se existem dados disponíveis para o período selecionado
4. O sistema deve informar claramente ao usuário caso os dados do período estejam incompletos ou indisponíveis

### 3. Relatório Exportável

Gera um relatório com os resultados do cálculo do gap, incluindo os componentes intermediários, exportável nos formatos PDF e Excel.

**Por que é importante**: Analistas precisam de documentos formais para embasar análises técnicas, e gestores precisam de relatórios para apresentações e tomada de decisão institucional.

**Requisitos funcionais**:
1. O relatório deve apresentar o ICMS potencial estimado do período
2. O relatório deve apresentar o ICMS efetivamente arrecadado do período
3. O relatório deve apresentar o VRR calculado
4. O relatório deve apresentar o valor do gap tributário em R$ e em percentual
5. O relatório deve ser exportável no formato PDF (layout profissional para apresentações)
6. O relatório deve ser exportável no formato Excel (para manipulação analítica)
7. O relatório deve identificar o período analisado, a data de geração e a metodologia utilizada (ESAF-2012/VRR OCDE)
8. O relatório deve incluir referência aos benchmarks internacionais para contextualização

### 4. Consolidação de Dados de Múltiplas Fontes

O sistema consolida dados de três fontes oficiais — importações, arrecadação estadual e comércio exterior — para viabilizar o cálculo do gap.

**Por que é importante**: O cálculo do gap requer cruzamento de dados de diferentes origens para estimar o ICMS potencial e compará-lo com o arrecadado efetivamente.

**Requisitos funcionais**:
1. O sistema deve consumir dados de importações (Siscomex)
2. O sistema deve consumir dados de arrecadação estadual do ICMS (GFIS2)
3. O sistema deve consumir dados de comércio exterior (MDIC ComEx)
4. O sistema deve validar a consistência e completude dos dados antes do cálculo
5. O sistema deve informar ao usuário caso alguma fonte de dados esteja indisponível ou com dados incompletos para o período

## User Experience

### Personas

**Analista Fiscal**: Profissional técnico da Sefaz MA que precisa de detalhes quantitativos do gap para embasar análises e relatórios técnicos. Necessita dos componentes intermediários do cálculo e exportação em formatos manipuláveis (Excel). Valoriza precisão e transparência metodológica.

**Gestor da Sefaz**: Tomador de decisão que precisa de uma visão consolidada e de alto nível do gap tributário para direcionar políticas de fiscalização e arrecadação. Prefere relatórios formatados (PDF) para apresentações institucionais. Valoriza clareza e contextualização dos resultados.

### Fluxo Principal

1. O usuário acessa a ferramenta de cálculo do gap tributário
2. O usuário seleciona o tipo de período desejado (trimestral ou anual)
3. O usuário seleciona o período específico (ex: Q3/2025 ou Ano 2025)
4. O sistema verifica a disponibilidade e completude dos dados para o período
5. O sistema executa o cálculo do gap aplicando a metodologia ESAF-2012/VRR
6. O sistema apresenta os resultados: ICMS potencial, ICMS arrecadado, VRR e valor do gap (R$ e %)
7. O usuário exporta o relatório no formato desejado (PDF ou Excel)

### Considerações de UX

- A interface deve ser simples e objetiva, priorizando a clareza dos resultados numéricos
- Valores monetários devem ser apresentados no formato brasileiro (R$, separadores de milhar com ponto, decimais com vírgula)
- O relatório PDF deve ter layout profissional adequado para apresentações institucionais da Sefaz
- O relatório Excel deve permitir manipulação dos dados pelo analista
- Mensagens de erro devem ser claras e específicas (ex: "Dados de importação indisponíveis para Q3/2025")
- O fluxo do cálculo deve ser linear e com poucos passos, minimizando a curva de aprendizado

## Non-Goals (Out of Scope)

- **Automação de recorrência**: O MVP não inclui execução automática periódica. O usuário executa o cálculo manualmente para o período desejado. A automação é planejada para versões futuras
- **Comparação histórica entre períodos**: O relatório apresenta apenas o resultado do período selecionado, sem gráficos de evolução ou comparação com períodos anteriores
- **Segmentação do gap**: O MVP calcula apenas o gap total consolidado do estado do Maranhão, sem quebras por setor econômico, tipo de operação, região ou município
- **Dashboard interativo**: O MVP entrega relatórios exportáveis (PDF/Excel), não um painel visual interativo com gráficos dinâmicos
- **Previsões e projeções**: O sistema calcula o gap com base em dados do período selecionado, não faz projeções futuras ou análises preditivas
- **Integração com sistemas de fiscalização**: O MVP não se integra com outros sistemas da Sefaz para disparar ações automáticas de fiscalização com base no gap
- **Gestão de usuários e permissões**: O MVP não inclui controle de acesso baseado em perfis ou papéis
- **Granularidade mensal**: O MVP suporta apenas períodos trimestrais e anuais, sem granularidade mensal