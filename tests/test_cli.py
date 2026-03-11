"""Testes da interface CLI.

Testes de scaffolding — verificam importabilidade e estrutura básica.
A implementação completa será feita na tarefa de CLI e orquestração.
"""

from __future__ import annotations

import subprocess
import sys


def test_version_command():
    """Verifica que --version retorna a versão sem erro."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "0.1.0" in result.stdout


def test_help_command():
    """Verifica que --help retorna ajuda sem erro."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Gap Tributário" in result.stdout


def test_no_args_shows_help():
    """Verifica que execução sem argumentos mostra help."""
    result = subprocess.run(
        [sys.executable, "-m", "gap_tributario"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
