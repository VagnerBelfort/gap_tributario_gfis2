"""Carregamento e gerenciamento de configuração da aplicação.

Responsável por:
- Carregar o arquivo YAML de alíquotas
- Resolver variáveis de ambiente (${VAR:-default})
- Expor ponto único de configuração para todos os módulos

Nenhum outro módulo deve ler env vars ou arquivos YAML diretamente.
"""

from __future__ import annotations

import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

import yaml

from gap_tributario.models import AppConfig, ConfigAliquota

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")


def _resolve_env_vars(value: str) -> str:
    """Resolve variáveis de ambiente no formato ${VAR:-default}.

    Args:
        value: String possivelmente contendo ${VAR:-default}

    Returns:
        String com variáveis resolvidas
    """

    def replace_match(match: re.Match) -> str:  # type: ignore[type-arg]
        var_name = match.group(1)
        default_value = match.group(2) or ""
        return os.environ.get(var_name, default_value)

    return _ENV_VAR_PATTERN.sub(replace_match, value)


def _resolve_dict_env_vars(data: Any) -> Any:
    """Resolve recursivamente variáveis de ambiente em um dicionário.

    Args:
        data: Estrutura de dados (dict, list ou escalar)

    Returns:
        Estrutura com variáveis de ambiente resolvidas
    """
    if isinstance(data, dict):
        return {k: _resolve_dict_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_dict_env_vars(item) for item in data]
    if isinstance(data, str):
        return _resolve_env_vars(data)
    return data


def load_config(config_path: str = "./config/aliquotas.yaml") -> AppConfig:
    """Carrega a configuração da aplicação a partir do arquivo YAML.

    Args:
        config_path: Caminho para o arquivo de configuração YAML

    Returns:
        AppConfig com todas as configurações carregadas

    Raises:
        FileNotFoundError: Se o arquivo de configuração não existir
        ValueError: Se a estrutura YAML for inválida
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        raw_data: Dict[str, Any] = yaml.safe_load(f)

    if raw_data is None:
        raise ValueError(f"Arquivo de configuração vazio: {config_path}")

    data = _resolve_dict_env_vars(raw_data)

    # Processar alíquotas
    if "aliquotas" not in data or not data["aliquotas"]:
        raise ValueError("Configuração deve conter pelo menos uma alíquota")

    aliquotas = []
    for item in data["aliquotas"]:
        aliquotas.append(
            ConfigAliquota(
                ano_inicio=int(item["ano_inicio"]),
                ano_fim=int(item["ano_fim"]) if item.get("ano_fim") is not None else None,
                aliquota=Decimal(str(item["aliquota"])),
                legislacao=str(item.get("legislacao", "")),
            )
        )

    # Processar fontes de dados
    fontes = data.get("fontes", {})
    parquet_base_path = Path(fontes.get("parquet_base_path", "./bases/g_arrecadacao/ouro/"))
    mdic_base_path = Path(fontes.get("mdic_base_path", "./mdic_comex/dados/"))

    # Processar configuração Oracle (opcional)
    oracle_config = data.get("oracle", {})
    oracle_dsn = oracle_config.get("dsn") or None
    oracle_user = oracle_config.get("user") or None
    oracle_password = oracle_config.get("password") or None

    # Diretório de saída (default)
    output_path = Path("./output/")

    return AppConfig(
        aliquotas=aliquotas,
        parquet_base_path=parquet_base_path,
        mdic_base_path=mdic_base_path,
        oracle_dsn=oracle_dsn,
        oracle_user=oracle_user,
        oracle_password=oracle_password,
        output_path=output_path,
    )
