from src.stats import _format_bytes


def test_format_bytes():
    assert _format_bytes(0) == "0 B"
    assert _format_bytes(512) == "512 B"
    assert _format_bytes(1024) == "1.0 KB"
    assert _format_bytes(1024 * 1024) == "1.0 MB"
    assert _format_bytes(1024 * 1024 * 1024) == "1.00 GB"
    assert _format_bytes(2 * 1024 ** 4) == "2.00 TB"
    assert _format_bytes(1536) == "1.5 KB"
