# Calculadora de Gap Tributário do ICMS-MA

Ferramenta CLI para cálculo do Gap Tributário do ICMS do estado do Maranhão usando a metodologia **VAT-VRR da OCDE**.

## Fórmula VRR (OCDE)

```
ICMS Potencial = (VAB - Exportações + Importações) × Alíquota Padrão
VRR = ICMS Arrecadado / ICMS Potencial
Gap = ICMS Potencial - ICMS Arrecadado
```

**Referência MA 2022:** VRR ≈ 0,52 (ICMS=10.917M, VAB=124.859M, Alíq=18%)

## Pré-requisitos

- Python 3.8+
- [uv](https://docs.astral.sh/uv/) instalado

### Instalar uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Instalação

```bash
# Clonar o repositório
git clone <url-do-repositorio>
cd gap_tributario_gfis2

# Instalar dependências (cria .venv automaticamente)
uv sync
```

### Com suporte Oracle Siscomex (opcional)

```bash
uv sync --extra oracle
```

## Uso

```bash
# Calcular gap anual de 2022
uv run python -m gap_tributario --periodo 2022

# Calcular gap do 1º trimestre de 2022
uv run python -m gap_tributario --periodo 2022-T1

# Gerar apenas PDF
uv run python -m gap_tributario --periodo 2022 --formato pdf

# Especificar diretório de saída
uv run python -m gap_tributario --periodo 2022 --saida ./relatorios/

# Habilitar enriquecimento via Siscomex Oracle
uv run python -m gap_tributario --periodo 2022 --siscomex

# Ver versão
uv run python -m gap_tributario --version

# Ajuda
uv run python -m gap_tributario --help
```

### Variáveis de Ambiente (Oracle Siscomex)

```bash
export ORACLE_DSN="10.1.1.132:1521/cent"
export ORACLE_USER="usuario"
export ORACLE_PASSWORD="senha"
```

## Fontes de Dados

| Fonte | Dado | Tipo |
|-------|------|------|
| GFIS2 Parquet (`bases/g_arrecadacao/ouro/`) | ICMS Arrecadado | Local |
| MDIC ComEx (`mdic_comex/dados/`) | Exportações/Importações | Local (CSV pré-baixado) |
| IBGE SIDRA API (Tabela 5938) | Valor Adicionado Bruto (VAB) | API pública |
| BCB PTAX API | Cotação USD/BRL | API pública |
| Oracle Siscomex (opcional) | Dados complementares | Oracle interno |

### Dados MDIC ComEx

Os CSVs do MDIC precisam ser baixados manualmente em:
https://balanca.economia.gov.br/

Salvar em `./mdic_comex/dados/` com nomenclatura:
- Exportações: `EXP_{ano}.csv`
- Importações: `IMP_{ano}.csv`

## Configuração

Editar `config/aliquotas.yaml` para ajustar alíquotas por período:

```yaml
aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Lei Estadual vigente até 2022"
  - ano_inicio: 2023
    ano_fim: null  # vigente
    aliquota: 0.20
    legislacao: "Lei 11.867/2022 — alíquota modal 20%"
```

## Desenvolvimento

```bash
# Rodar testes
uv run pytest tests/

# Verificar linting
uv run ruff check src/

# Formatar código
uv run ruff format src/

# Cobertura de testes
uv run pytest tests/ --cov=src/gap_tributario --cov-report=html
```

## Estrutura do Projeto

```
gap_tributario_gfis2/
├── src/
│   └── gap_tributario/
│       ├── __init__.py          # Versão do pacote
│       ├── __main__.py          # Entrypoint: python -m gap_tributario
│       ├── cli.py               # Interface CLI (argparse)
│       ├── config.py            # Carregamento de configuração YAML
│       ├── models.py            # Dataclasses de dados
│       ├── extractors/          # Extratores por fonte de dados
│       ├── validators/          # Schemas Pandera (validação)
│       ├── engine/              # Motor de cálculo VRR
│       └── report/              # Geradores de relatório (PDF/Excel)
├── config/
│   └── aliquotas.yaml           # Alíquotas ICMS-MA por período
├── tests/                       # Testes automatizados
├── pyproject.toml               # Dependências gerenciadas por uv
└── README.md
```

## Códigos de Saída

| Código | Significado |
|--------|-------------|
| 0 | Sucesso — relatório(s) gerado(s) |
| 1 | Erro de validação (Pandera) |
| 2 | Erro de extração (fonte indisponível) |
| 3 | Erro de configuração |
| 4 | Erro de argumento CLI |
