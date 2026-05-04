"""Tests for src/volume_files.py — Volume file walker."""

from unittest.mock import MagicMock

from src.volume_files import FileEntry, list_files, total_size_bytes


def _file(path: str, size: int = 100, mtime: int = 1700000000000) -> MagicMock:
    """Build a fake `DirectoryEntry` shaped like the SDK returns."""
    e = MagicMock()
    e.path = path
    e.is_directory = False
    e.file_size = size
    e.modification_time = mtime
    return e


def _dir(path: str) -> MagicMock:
    e = MagicMock()
    e.path = path
    e.is_directory = True
    return e


def test_list_files_returns_files_at_root_only():
    """Flat Volume directory — three files, no sub-dirs. Returns a
    FileEntry per file with the SDK's path / size / mtime carried
    through."""
    client = MagicMock()
    client.files.list_directory_contents.return_value = [
        _file("/Volumes/c/s/v/a.parquet", size=200),
        _file("/Volumes/c/s/v/b.parquet", size=300),
        _file("/Volumes/c/s/v/c.parquet", size=400),
    ]
    out = list_files(client, "/Volumes/c/s/v/")
    assert len(out) == 3
    assert {e.path for e in out} == {
        "/Volumes/c/s/v/a.parquet",
        "/Volumes/c/s/v/b.parquet",
        "/Volumes/c/s/v/c.parquet",
    }
    assert sum(e.size_bytes for e in out) == 900


def test_list_files_recurses_into_sub_directories():
    """Mirrors the walk in `_copy_volume_files` — depth-first into
    every sub-dir, files-only in the result. The Spark-format convert
    typically writes one sub-dir of part-files per export, so this
    case is the dominant production shape."""
    client = MagicMock()

    def fake_list(directory_path: str):
        if directory_path == "/Volumes/c/s/v/":
            return [
                _dir("/Volumes/c/s/v/sub1"),
                _file("/Volumes/c/s/v/_SUCCESS", size=0),
            ]
        if directory_path == "/Volumes/c/s/v/sub1":
            return [
                _file("/Volumes/c/s/v/sub1/part-00000.parquet", size=1024),
                _file("/Volumes/c/s/v/sub1/part-00001.parquet", size=2048),
            ]
        return []

    client.files.list_directory_contents.side_effect = fake_list
    out = list_files(client, "/Volumes/c/s/v/")
    assert len(out) == 3
    assert sum(e.size_bytes for e in out) == 1024 + 2048 + 0


def test_list_files_fails_open_on_locked_subdir():
    """If listing a sub-directory raises (perms / transient), the walk
    must continue from the next sibling rather than blanking the whole
    listing — matches the same fail-open posture the clone path uses
    when copying."""
    client = MagicMock()

    def fake_list(directory_path: str):
        if directory_path == "/Volumes/c/s/v/":
            return [
                _dir("/Volumes/c/s/v/sub_locked"),
                _dir("/Volumes/c/s/v/sub_open"),
            ]
        if directory_path == "/Volumes/c/s/v/sub_locked":
            raise PermissionError("denied")
        if directory_path == "/Volumes/c/s/v/sub_open":
            return [_file("/Volumes/c/s/v/sub_open/ok.parquet", size=500)]
        return []

    client.files.list_directory_contents.side_effect = fake_list
    out = list_files(client, "/Volumes/c/s/v/")
    # The locked dir contributes nothing; the open dir contributes its
    # one file. The whole walk doesn't crash on the PermissionError.
    assert len(out) == 1
    assert out[0].path == "/Volumes/c/s/v/sub_open/ok.parquet"


def test_list_files_returns_empty_when_root_unlistable():
    """Bad / non-existent root → empty list, not a crash. The API
    surface translates this into a 200 with `count=0` so the operator
    can investigate."""
    client = MagicMock()
    client.files.list_directory_contents.side_effect = Exception("not found")
    assert list_files(client, "/Volumes/c/s/missing/") == []


def test_total_size_bytes_sums_entries():
    entries = [
        FileEntry(path="/a", size_bytes=100),
        FileEntry(path="/b", size_bytes=200),
        FileEntry(path="/c", size_bytes=0),
    ]
    assert total_size_bytes(entries) == 300


def test_total_size_bytes_empty_list_is_zero():
    assert total_size_bytes([]) == 0


def test_list_files_handles_missing_size_attribute_as_zero():
    """Some SDK responses omit `file_size` for very recently written
    files. Default to 0 so the walk doesn't crash with AttributeError."""
    client = MagicMock()
    item_no_size = MagicMock()
    item_no_size.path = "/Volumes/c/s/v/x.parquet"
    item_no_size.is_directory = False
    item_no_size.file_size = None  # SDK reports None when size is unknown
    item_no_size.modification_time = None
    client.files.list_directory_contents.return_value = [item_no_size]
    out = list_files(client, "/Volumes/c/s/v/")
    assert len(out) == 1
    assert out[0].size_bytes == 0
