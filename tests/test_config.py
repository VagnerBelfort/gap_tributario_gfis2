"""Testes unitários para src/gap_tributario/config.py.

Cobre load_config(), resolução de env vars e validação do YAML.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest

from gap_tributario.config import load_config

YAML_VALIDO_COMPLETO = """\
aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Lei Estadual vigente até 2022"
  - ano_inicio: 2023
    ano_fim: null
    aliquota: 0.20
    legislacao: "Lei 11.867/2022 — alíquota modal 20%"

fontes:
  parquet_base_path: "./bases/g_arrecadacao/ouro/"
  mdic_base_path: "./mdic_comex/dados/"

oracle:
  dsn: ""
  user: ""
  password: ""
"""

YAML_COM_ENV_VARS = """\
aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Teste"

oracle:
  dsn: "${ORACLE_DSN:-}"
  user: "${ORACLE_USER:-}"
  password: "${ORACLE_PASSWORD:-}"
"""

YAML_SEM_FONTES = """\
aliquotas:
  - ano_inicio: 2010
    ano_fim: 2022
    aliquota: 0.18
    legislacao: "Teste"
"""


class TestLoadConfig:
    """Testes para a função load_config()."""

    def test_yaml_valido_completo_carrega_sem_erros(self, tmp_path: Path) -> None:
        """YAML válido completo deve ser carregado sem levantar exceções."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        assert config is not None

    def test_yaml_valido_carrega_todas_as_secoes(self, tmp_path: Path) -> None:
        """YAML completo deve carregar alíquotas, fontes e oracle."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        assert len(config.aliquotas) > 0
        assert config.parquet_base_path is not None
        assert config.mdic_base_path is not None

    def test_numero_correto_de_aliquotas(self, tmp_path: Path) -> None:
        """YAML padrão deve carregar exatamente 2 entradas de alíquotas."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        assert len(config.aliquotas) == 2

    def test_aliquota_convertida_para_decimal(self, tmp_path: Path) -> None:
        """Alíquota deve ser convertida para Decimal, não float."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        for aliq in config.aliquotas:
            assert isinstance(aliq.aliquota, Decimal), (
                f"Alíquota deveria ser Decimal, mas é {type(aliq.aliquota)}"
            )

    def test_arquivo_ausente_levanta_file_not_found_error(self, tmp_path: Path) -> None:
        """Arquivo ausente deve levantar FileNotFoundError com path na mensagem."""
        caminho_invalido = str(tmp_path / "nao_existe.yaml")

        with pytest.raises(FileNotFoundError) as exc_info:
            load_config(caminho_invalido)

        assert caminho_invalido in str(exc_info.value)

    def test_yaml_vazio_levanta_value_error(self, tmp_path: Path) -> None:
        """YAML vazio deve levantar ValueError."""
        yaml_vazio = tmp_path / "vazio.yaml"
        yaml_vazio.write_text("", encoding="utf-8")

        with pytest.raises(ValueError):
            load_config(str(yaml_vazio))

    def test_yaml_sem_chave_aliquotas_levanta_value_error(self, tmp_path: Path) -> None:
        """YAML sem chave 'aliquotas' deve levantar ValueError."""
        yaml_sem_aliquotas = tmp_path / "sem_aliquotas.yaml"
        yaml_sem_aliquotas.write_text(
            "fontes:\n  parquet_base_path: ./bases\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError):
            load_config(str(yaml_sem_aliquotas))

    def test_yaml_com_lista_aliquotas_vazia_levanta_value_error(self, tmp_path: Path) -> None:
        """YAML com lista 'aliquotas' vazia deve levantar ValueError."""
        yaml_aliq_vazia = tmp_path / "aliq_vazia.yaml"
        yaml_aliq_vazia.write_text("aliquotas: []\n", encoding="utf-8")

        with pytest.raises(ValueError):
            load_config(str(yaml_aliq_vazia))

    def test_resolucao_env_var_nao_definida_retorna_none(self, tmp_path: Path) -> None:
        """${ORACLE_DSN:-} deve resultar em oracle_dsn=None quando env var não definida."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(YAML_COM_ENV_VARS, encoding="utf-8")

        env_backup = os.environ.pop("ORACLE_DSN", None)
        try:
            config = load_config(str(yaml_file))
            # String vazia resultante de ${VAR:-} sem default deve ser convertida para None
            assert config.oracle_dsn is None
        finally:
            if env_backup is not None:
                os.environ["ORACLE_DSN"] = env_backup

    def test_resolucao_env_var_definida_retorna_valor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """${ORACLE_DSN:-} deve retornar o valor da env var quando definida."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(YAML_COM_ENV_VARS, encoding="utf-8")

        dsn_esperado = "10.1.1.132:1521/cent"
        monkeypatch.setenv("ORACLE_DSN", dsn_esperado)

        config = load_config(str(yaml_file))

        assert config.oracle_dsn == dsn_esperado

    def test_credencial_oracle_vazia_resulta_em_none(self, tmp_path: Path) -> None:
        """Credencial Oracle vazia ('') deve resultar em oracle_dsn=None no AppConfig."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        assert config.oracle_dsn is None
        assert config.oracle_user is None
        assert config.oracle_password is None

    def test_path_default_quando_fontes_ausentes(self, tmp_path: Path) -> None:
        """Path default deve ser usado quando seção 'fontes' está ausente."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(YAML_SEM_FONTES, encoding="utf-8")

        config = load_config(str(yaml_file))

        assert "g_arrecadacao" in str(config.parquet_base_path)
        assert "mdic_comex" in str(config.mdic_base_path)

    def test_fixture_aliquotas_test_yaml_carrega_sem_erros(self) -> None:
        """Fixture tests/fixtures/config/aliquotas_test.yaml deve carregar sem erros."""
        fixture_path = Path("tests/fixtures/config/aliquotas_test.yaml")

        if not fixture_path.exists():
            pytest.skip("Fixture aliquotas_test.yaml não encontrada")

        config = load_config(str(fixture_path))

        assert config is not None
        assert len(config.aliquotas) >= 1

    def test_yaml_fixture_via_conftest(self, config_yaml_path: Path) -> None:
        """Fixture config_yaml_path do conftest deve carregar sem erros."""
        config = load_config(str(config_yaml_path))

        assert config is not None
        assert len(config.aliquotas) == 2

    def test_aliquotas_primeira_entrada(self, tmp_path: Path) -> None:
        """Primeira alíquota deve corresponder ao período 2010-2022 com 0.18."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        primeira = config.aliquotas[0]
        assert primeira.ano_inicio == 2010
        assert primeira.ano_fim == 2022
        assert primeira.aliquota == Decimal("0.18")

    def test_aliquotas_segunda_entrada_vigente(self, tmp_path: Path) -> None:
        """Segunda alíquota deve ser a vigente (ano_fim=None) com 0.20."""
        yaml_file = tmp_path / "aliquotas.yaml"
        yaml_file.write_text(YAML_VALIDO_COMPLETO, encoding="utf-8")

        config = load_config(str(yaml_file))

        segunda = config.aliquotas[1]
        assert segunda.ano_inicio == 2023
        assert segunda.ano_fim is None
        assert segunda.aliquota == Decimal("0.20")

    def test_load_config_com_path_default(self) -> None:
        """load_config() com path default deve usar ./config/aliquotas.yaml."""
        config_path = Path("./config/aliquotas.yaml")

        if not config_path.exists():
            pytest.skip("Arquivo config/aliquotas.yaml não existe no diretório atual")

        config = load_config()

        assert config is not None
        assert len(config.aliquotas) >= 1
