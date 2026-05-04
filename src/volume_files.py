"""List file objects inside a Unity Catalog Volume.

The convert export-to-Volume path writes file objects (Parquet / Avro /
ORC / JSON) into a Volume sub-directory. After a successful export the
UI wants a "N files written, X MB" indicator so the operator knows the
export actually produced data — without a count, the cart row says
"converted" but it's not clear whether 0 bytes or 100 GB landed.

This module exposes a single helper, :func:`list_files`, which walks
the Volume tree under a given path and returns every file (recursing
into sub-directories). It intentionally does NOT cover the
download/upload responsibilities of
:func:`src.clone_cross_workspace._copy_volume_files` — that function
copies files between workspaces; this one only enumerates them. Two
different responsibilities, two different functions.

Why a standalone module: the recursive walk used to be inlined in the
clone module's copy loop. Pulling it out lets the convert UI's
post-export count reuse the exact same listing semantics that the
clone path uses to decide which files to copy, so what one sees is
what the other would copy.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@dataclass
class FileEntry:
    """One file inside a Volume.

    ``path`` is the absolute Volume URI (``/Volumes/<cat>/<sch>/<vol>/...``).
    ``size_bytes`` is the file's byte count as the SDK reports it; falls
    back to ``0`` when the SDK doesn't surface a size (some directory
    listings omit it for very recently-written files).
    ``modification_time`` is a millisecond UTC epoch when present, else
    ``None``.
    """

    path: str
    size_bytes: int = 0
    modification_time: int | None = None


def list_files(client: WorkspaceClient, root_path: str) -> list[FileEntry]:
    """Return every file under ``root_path`` (recursive).

    Mirrors the walk semantics in
    ``src.clone_cross_workspace._copy_volume_files`` so a "what would
    be copied" preview lines up with the actual copy. Directories
    don't appear in the returned list — only files. Sub-directories
    are walked depth-first.

    ``root_path`` must be a Volume URI. Behaviour against non-Volume
    paths (DBFS, workspace paths) is undefined — the SDK call will
    likely fail with a permissions error.

    Failures from individual ``list_directory_contents`` calls are
    logged at DEBUG and the walk continues from the next sibling, so a
    single locked sub-directory doesn't blank the whole listing.
    """
    out: list[FileEntry] = []

    def _walk(path: str) -> None:
        try:
            contents = client.files.list_directory_contents(directory_path=path)
        except Exception as e:
            # A locked / non-existent sub-directory shouldn't blank the
            # whole walk — log and continue. The caller sees the
            # partial listing; if the root itself is bad they'll get
            # an empty list, which the API surface translates into a
            # 200 with `count=0` (operator can investigate).
            logger.debug(f"list_directory_contents({path}) failed: {e}")
            return
        for item in contents:
            item_path = item.path
            if getattr(item, "is_directory", False):
                _walk(item_path)
            else:
                out.append(
                    FileEntry(
                        path=item_path,
                        size_bytes=getattr(item, "file_size", 0) or 0,
                        modification_time=getattr(item, "modification_time", None),
                    )
                )

    _walk(root_path)
    return out


def total_size_bytes(entries: list[FileEntry]) -> int:
    """Sum file sizes across a listing. Trivial helper, but lifted out
    so the API + UI both render the same number from the same source.
    """
    return sum(e.size_bytes for e in entries)
