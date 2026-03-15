from src.profiling import _safe_int


def test_safe_int_valid():
    assert _safe_int(42) == 42
    assert _safe_int("100") == 100
    assert _safe_int(0) == 0


def test_safe_int_none():
    assert _safe_int(None) is None


def test_safe_int_invalid():
    assert _safe_int("not_a_number") is None
    assert _safe_int([]) is None
