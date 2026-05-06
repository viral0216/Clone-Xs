"""Tests for api/routers/files.py — GET /api/files/list."""

from unittest.mock import MagicMock


def _file(path: str, size: int = 100) -> MagicMock:
    e = MagicMock()
    e.path = path
    e.is_directory = False
    e.file_size = size
    e.modification_time = 1700000000000
    return e


def test_list_files_returns_count_and_total_size(client, mock_workspace_client):
    """Round-trip: walk a Volume path, return per-file entries plus
    aggregate count + total bytes. The UI reads count + total_bytes
    directly to render the "N files · X MB" chip without
    re-aggregating client-side."""
    mock_workspace_client.files.list_directory_contents.return_value = [
        _file("/Volumes/c/s/v/sub/a.parquet", size=1024),
        _file("/Volumes/c/s/v/sub/b.parquet", size=2048),
    ]
    resp = client.get("/api/files/list?path=/Volumes/c/s/v/sub/")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["path"] == "/Volumes/c/s/v/sub/"
    assert body["count"] == 2
    assert body["total_size_bytes"] == 1024 + 2048
    paths = {f["path"] for f in body["files"]}
    assert paths == {
        "/Volumes/c/s/v/sub/a.parquet",
        "/Volumes/c/s/v/sub/b.parquet",
    }


def test_list_files_empty_volume_returns_count_zero(client, mock_workspace_client):
    """A Volume the convert "succeeded" against but produced no files
    (operator misconfiguration, schema mismatch silently dropped rows,
    etc.) returns 200 with count=0 — the UI surfaces this as "0 files
    written" so the operator notices instead of trusting the green
    badge alone."""
    mock_workspace_client.files.list_directory_contents.return_value = []
    resp = client.get("/api/files/list?path=/Volumes/c/s/v/empty/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["total_size_bytes"] == 0
    assert body["files"] == []


def test_list_files_rejects_non_volume_path(client):
    """DBFS / workspace paths are out of scope. Reject early with a
    clean 400 rather than letting the SDK surface an opaque error
    deep in the stack."""
    resp = client.get("/api/files/list?path=/dbfs/some/path")
    assert resp.status_code == 400
    detail = resp.json()["detail"].lower()
    assert "/volumes/" in detail


def test_list_files_missing_path_param_returns_422(client):
    """`path` is required — FastAPI returns 422 on a missing query
    param; pin that contract so the UI's catch-422 path keeps working."""
    resp = client.get("/api/files/list")
    assert resp.status_code == 422


def test_list_files_passes_through_modification_time(client, mock_workspace_client):
    """SDK's `modification_time` (millis-since-epoch) round-trips into
    the response so callers can render "last written N minutes ago"
    if they want to."""
    f = _file("/Volumes/c/s/v/x.parquet", size=10)
    f.modification_time = 1735689600000  # 2025-01-01 UTC
    mock_workspace_client.files.list_directory_contents.return_value = [f]
    resp = client.get("/api/files/list?path=/Volumes/c/s/v/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["files"][0]["modification_time"] == 1735689600000
