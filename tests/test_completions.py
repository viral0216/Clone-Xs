from src.completions import (
    generate_bash_completion,
    generate_fish_completion,
    generate_zsh_completion,
)


def test_bash_completion_contains_commands():
    script = generate_bash_completion()
    assert "clone" in script
    assert "preflight" in script
    assert "search" in script
    assert "monitor" in script
    assert "export" in script
    assert "complete -F" in script


def test_zsh_completion_contains_commands():
    script = generate_zsh_completion()
    assert "clone" in script
    assert "preflight" in script
    assert "_clxs" in script


def test_fish_completion_contains_commands():
    script = generate_fish_completion()
    assert "clone" in script
    assert "preflight" in script
    assert "complete -c clxs" in script
