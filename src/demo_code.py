"""Synthetic source-code repo generators for the /demo-data Code tab.

Pairs with the other unstructured generators — same registry,
orchestrator, and destination-radio pattern. Different on two axes:

  - **One "count" = one full repo, not one file.** Each repo is a
    directory tree of ~25-35 source files (src/, tests/, manifest,
    README). When the operator picks "5 python_repos" they get 5
    independent project trees, each with its own name.
  - **direct_table is one row per source FILE** (text-shaped, so
    `content STRING` not BINARY). Code-search embeddings work at the
    file level so this is the natural shape — embeddings can be
    added as a sibling ARRAY<FLOAT> column without re-reading from
    a Volume.

Three generators (one per language):

  - **python_repo** — src/, tests/, README.md, pyproject.toml.
    Synthetic but plausible-looking modules with imports + classes
    + functions. NOT runnable — content is template-driven for
    code-search demos, not for execution.
  - **js_repo** — src/, package.json. Synthetic Node/JS modules.
  - **java_repo** — src/main/java/com/example/, pom.xml. Synthetic
    classes following Java conventions.

No optional Python deps — content is template-driven. AI mode (when
enabled and an AI client is available) drafts function bodies for
more interesting embeddings; without it, body content is templated
with Faker-generated identifiers.
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ── Lazy import probe ──────────────────────────────────────────────
#
# Code has no optional Python deps — pure stdlib + Faker.

CODE_AVAILABLE: bool = True
_UNAVAILABLE_REASON: str | None = None


def is_available() -> tuple[bool, str | None]:
    return CODE_AVAILABLE, _UNAVAILABLE_REASON


# ── Type registry ──────────────────────────────────────────────────

CODE_TYPES: dict[str, dict[str, str]] = {
    "python_repo": {
        "category": "Python",
        "label": "Python project (src/, tests/, README, pyproject.toml)",
        "extension": "py",  # primary file extension; repo also has .md / .toml
        "language": "python",
        "gen_fn": "_gen_python_repo",
    },
    "js_repo": {
        "category": "JavaScript",
        "label": "Node/JS project (src/, package.json)",
        "extension": "js",
        "language": "javascript",
        "gen_fn": "_gen_js_repo",
    },
    "java_repo": {
        "category": "Java",
        "label": "Maven project (src/main/java/, pom.xml)",
        "extension": "java",
        "language": "java",
        "gen_fn": "_gen_java_repo",
    },
}


# Empirical averages for /preview. Each repo ~25-35 files; each file
# ~1.5KB on average.
_AVG_FILES_PER_REPO: dict[str, int] = {
    "python_repo": 30,
    "js_repo": 28,
    "java_repo": 32,
}
_AVG_BYTES_PER_FILE: dict[str, int] = {
    "python_repo": 1_400,
    "js_repo": 1_200,
    "java_repo": 1_800,
}
# Repos generated per second (template-driven is fast; AI mode is
# slower but the preview can't predict that without knowing if the
# AI client is wired up — assume template speed).
_REPOS_PER_SECOND: dict[str, int] = {
    "python_repo": 8,
    "js_repo": 8,
    "java_repo": 6,
}


# ── Industry-aware repo naming ────────────────────────────────────

_INDUSTRY_REPO_PREFIXES: dict[str, list[str]] = {
    "healthcare": ["patient", "ehr", "billing", "clinical", "labs", "telemedicine"],
    "financial": ["payments", "ledger", "fraud", "kyc", "trading", "risk"],
    "retail": ["checkout", "catalog", "inventory", "loyalty", "promo", "shipping"],
    "telecom": ["billing", "provisioning", "session", "tower", "device"],
    "manufacturing": ["mes", "scada", "qc", "yield", "downtime"],
    "energy": ["grid", "outage", "smartmeter", "demand", "evcharger"],
    "education": ["lms", "enrollment", "grades", "alerts"],
    "real_estate": ["listings", "showings", "mortgage", "leads"],
    "logistics": ["tracking", "route", "fleet", "yard", "claims"],
    "insurance": ["claims", "underwriting", "policy", "fnol"],
}
_REPO_SUFFIXES = ["service", "api", "gateway", "engine", "core", "platform", "lib", "sdk"]


def _make_repo_name(industry: str) -> str:
    prefixes = _INDUSTRY_REPO_PREFIXES.get(industry, ["app", "service", "platform"])
    return f"{random.choice(prefixes)}-{random.choice(_REPO_SUFFIXES)}-{uuid.uuid4().hex[:6]}"


# ── Identifier pools ──────────────────────────────────────────────

_VERBS = [
    "calculate",
    "validate",
    "process",
    "transform",
    "fetch",
    "build",
    "render",
    "compute",
    "merge",
    "filter",
    "score",
    "normalize",
]
_NOUNS = [
    "record",
    "result",
    "request",
    "response",
    "payload",
    "entity",
    "snapshot",
    "summary",
    "context",
    "report",
    "metric",
]
_ADJECTIVES = ["active", "pending", "validated", "raw", "normalized", "scored", "enriched"]


def _camel(parts: list[str]) -> str:
    """CamelCase ('paymentRecord')."""
    return parts[0] + "".join(p.title() for p in parts[1:])


def _pascal(parts: list[str]) -> str:
    """PascalCase ('PaymentRecord')."""
    return "".join(p.title() for p in parts)


def _snake(parts: list[str]) -> str:
    """snake_case ('payment_record')."""
    return "_".join(parts)


def _func_name() -> str:
    return _snake([random.choice(_VERBS), random.choice(_NOUNS)])


def _class_name() -> str:
    return _pascal([random.choice(_ADJECTIVES), random.choice(_NOUNS)])


# ── Per-language file generators ──────────────────────────────────


def _maybe_ai_body(ai_client: Any | None, prompt: str, fallback: str) -> str:
    """Draft a function/class body via the shared AI adapter if
    available; otherwise return a templated fallback.

    Delegates to :class:`src.ai_drafter.AIDrafter.draft` (which handles
    the Databricks/Anthropic backend selection, token budgeting, and
    error fallback)."""
    if ai_client is None:
        return fallback
    return ai_client.draft(prompt, fallback=fallback, max_tokens=300)


def _gen_python_module(industry: str, fkr: Any, ai_client: Any | None) -> tuple[str, str]:
    """Synthesise one Python source file. Returns (filename, content)."""
    cls = _class_name()
    func1 = _func_name()
    func2 = _func_name()
    constant = _snake([random.choice(_ADJECTIVES), random.choice(_NOUNS)]).upper()

    body1 = _maybe_ai_body(
        ai_client,
        f"Write a 5-line Python function body for {func1}() in a {industry} {cls} class.",
        f'    """Process the {industry} {random.choice(_NOUNS)} and return the result."""\n'
        f"    if not data:\n"
        f"        return None\n"
        f"    enriched = {{**data, 'updated_at': _utcnow()}}\n"
        f"    return enriched",
    )
    body2 = _maybe_ai_body(
        ai_client,
        f"Write a 4-line Python function body for {func2}() in a {industry} {cls} class.",
        "    total = 0\n"
        "    for item in items:\n"
        "        total += item.get('amount', 0)\n"
        "    return total",
    )

    content = f'''"""{cls} — internal {industry} module.

Auto-generated demo asset. Not runnable.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

{constant} = {random.randint(10, 1000)}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class {cls}:
    """Encapsulates {industry} {random.choice(_NOUNS)} handling."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._cache: dict[str, Any] = {{}}

    def {func1}(self, data: dict[str, Any]) -> dict[str, Any] | None:
{body1}

    def {func2}(self, items: list[dict[str, Any]]) -> int:
{body2}
'''
    filename = _snake([random.choice(_VERBS), random.choice(_NOUNS), uuid.uuid4().hex[:6]]) + ".py"
    return filename, content


def _gen_python_test(module_name: str) -> tuple[str, str]:
    """Synthesise one pytest file pointed at `module_name`."""
    func_to_test = _func_name()
    content = f'''"""Tests for {module_name}."""

import pytest

from {module_name} import {func_to_test}


def test_{func_to_test}_returns_expected_shape():
    result = {func_to_test}({{"id": 1, "amount": 42}})
    assert result is not None
    assert "amount" in result


def test_{func_to_test}_handles_empty_input():
    result = {func_to_test}({{}})
    assert result is None or result == {{}}
'''
    filename = f"test_{module_name}.py"
    return filename, content


def _gen_python_repo(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[list[tuple[str, str]], dict]:
    """Generate one Python repo. Returns (files, repo_meta) where
    files is a list of (relative_path, content) tuples and repo_meta
    is the metadata for the repo as a whole."""
    repo_name = _make_repo_name(industry)
    src_count = random.randint(15, 25)
    test_count = random.randint(5, 10)

    files: list[tuple[str, str]] = []
    src_module_names: list[str] = []

    # README
    files.append(
        (
            "README.md",
            f"# {repo_name}\n\nAuto-generated demo asset for the {industry} industry. "
            f"This repo is not runnable; the source files are templates for "
            f"code-search / Copilot-style demos.\n",
        )
    )
    # pyproject.toml
    files.append(
        (
            "pyproject.toml",
            f'''[project]
name = "{repo_name}"
version = "0.1.0"
description = "Synthetic {industry} demo project"
requires-python = ">=3.10"

[tool.pytest.ini_options]
testpaths = ["tests"]
''',
        )
    )

    # src/ modules
    for _ in range(src_count):
        fname, content = _gen_python_module(industry, fkr, ai_client)
        files.append((f"src/{repo_name.replace('-', '_')}/{fname}", content))
        src_module_names.append(fname[:-3])  # strip .py

    # tests/ files (each pointed at a real src module name so the
    # imports look plausible)
    for _ in range(test_count):
        target = random.choice(src_module_names) if src_module_names else "module"
        fname, content = _gen_python_test(target)
        files.append((f"tests/{fname}", content))

    total_bytes = sum(len(c.encode("utf-8")) for _, c in files)
    return files, {
        "repo_name": repo_name,
        "language": "python",
        "file_count": len(files),
        "src_count": src_count,
        "test_count": test_count,
        "total_bytes": total_bytes,
    }


def _gen_js_module(industry: str, fkr: Any, ai_client: Any | None) -> tuple[str, str]:
    """One JavaScript module."""
    cls = _class_name()
    func1 = _camel([random.choice(_VERBS), random.choice(_NOUNS)])
    func2 = _camel([random.choice(_VERBS), random.choice(_NOUNS)])
    constant = _snake([random.choice(_ADJECTIVES), random.choice(_NOUNS)]).upper()

    body = _maybe_ai_body(
        ai_client,
        f"Write a 5-line JavaScript function body for {func1}() in a {industry} {cls} module.",
        "  if (!data) return null;\n  return { ...data, updatedAt: new Date().toISOString() };",
    )

    content = f"""// {cls} — internal {industry} module.
// Auto-generated demo asset. Not runnable.

const {constant} = {random.randint(10, 1000)};

class {cls} {{
  constructor(config) {{
    this.config = config;
    this._cache = new Map();
  }}

  {func1}(data) {{
{body}
  }}

  {func2}(items) {{
    return items.reduce((sum, item) => sum + (item.amount ?? 0), 0);
  }}
}}

module.exports = {{ {cls}, {constant} }};
"""
    filename = _camel([random.choice(_VERBS), random.choice(_NOUNS), uuid.uuid4().hex[:6]]) + ".js"
    return filename, content


def _gen_js_test(module_name: str) -> tuple[str, str]:
    """One JS Jest-style test file."""
    func = _camel([random.choice(_VERBS), random.choice(_NOUNS)])
    content = f"""const {{ {func} }} = require('../src/{module_name}');

describe('{module_name}', () => {{
  it('handles a populated payload', () => {{
    const result = {func}({{ id: 1, amount: 42 }});
    expect(result).not.toBeNull();
  }});

  it('handles empty input', () => {{
    expect(() => {func}(null)).not.toThrow();
  }});
}});
"""
    filename = f"{module_name}.test.js"
    return filename, content


def _gen_js_repo(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[list[tuple[str, str]], dict]:
    repo_name = _make_repo_name(industry)
    src_count = random.randint(14, 22)
    test_count = random.randint(4, 8)

    files: list[tuple[str, str]] = []
    src_module_names: list[str] = []

    files.append(
        (
            "README.md",
            f"# {repo_name}\n\nAuto-generated {industry} demo project. Not runnable.\n",
        )
    )
    files.append(
        (
            "package.json",
            json.dumps(
                {
                    "name": repo_name,
                    "version": "0.1.0",
                    "description": f"Synthetic {industry} demo project",
                    "main": "src/index.js",
                    "scripts": {"test": "jest"},
                    "devDependencies": {"jest": "^29.0.0"},
                },
                indent=2,
            )
            + "\n",
        )
    )

    for _ in range(src_count):
        fname, content = _gen_js_module(industry, fkr, ai_client)
        files.append((f"src/{fname}", content))
        src_module_names.append(fname[:-3])

    for _ in range(test_count):
        target = random.choice(src_module_names) if src_module_names else "index"
        fname, content = _gen_js_test(target)
        files.append((f"tests/{fname}", content))

    total_bytes = sum(len(c.encode("utf-8")) for _, c in files)
    return files, {
        "repo_name": repo_name,
        "language": "javascript",
        "file_count": len(files),
        "src_count": src_count,
        "test_count": test_count,
        "total_bytes": total_bytes,
    }


def _gen_java_class(
    industry: str, fkr: Any, ai_client: Any | None, package: str
) -> tuple[str, str]:
    """One Java class."""
    cls = _pascal([random.choice(_ADJECTIVES), random.choice(_NOUNS)])
    method1 = _camel([random.choice(_VERBS), random.choice(_NOUNS)])
    method2 = _camel([random.choice(_VERBS), random.choice(_NOUNS)])
    constant = _snake([random.choice(_ADJECTIVES), random.choice(_NOUNS)]).upper()

    body = _maybe_ai_body(
        ai_client,
        f"Write a 5-line Java method body for {method1}() in a {industry} {cls} class.",
        "        if (data == null) {\n"
        "            return null;\n"
        "        }\n"
        "        return new HashMap<>(data);",
    )

    content = f"""package {package};

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {cls} — internal {industry} module.
 * Auto-generated demo asset. Not runnable.
 */
public class {cls} {{

    public static final int {constant} = {random.randint(10, 1000)};

    private final Map<String, Object> config;

    public {cls}(Map<String, Object> config) {{
        this.config = config;
    }}

    public Map<String, Object> {method1}(Map<String, Object> data) {{
{body}
    }}

    public long {method2}(List<Map<String, Object>> items) {{
        long total = 0;
        for (Map<String, Object> item : items) {{
            Object amount = item.get("amount");
            if (amount instanceof Number) {{
                total += ((Number) amount).longValue();
            }}
        }}
        return total;
    }}
}}
"""
    filename = f"{cls}.java"
    return filename, content


def _gen_java_test(package: str, target_class: str) -> tuple[str, str]:
    test_class = f"{target_class}Test"
    content = f"""package {package};

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.Map;

public class {test_class} {{

    @Test
    public void shouldHandlePopulatedPayload() {{
        Map<String, Object> config = new HashMap<>();
        {target_class} subject = new {target_class}(config);
        Map<String, Object> input = new HashMap<>();
        input.put("amount", 42);
        assertNotNull(subject);
    }}
}}
"""
    return f"{test_class}.java", content


def _gen_java_repo(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[list[tuple[str, str]], dict]:
    repo_name = _make_repo_name(industry)
    package = "com.example." + industry.replace("_", "")
    package_path = "src/main/java/" + package.replace(".", "/")
    test_package_path = "src/test/java/" + package.replace(".", "/")
    src_count = random.randint(15, 25)
    test_count = random.randint(5, 8)

    files: list[tuple[str, str]] = []
    class_names: list[str] = []

    files.append(
        (
            "README.md",
            f"# {repo_name}\n\nAuto-generated {industry} Java demo project. Not buildable.\n",
        )
    )
    files.append(
        (
            "pom.xml",
            f"""<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>{repo_name}</artifactId>
    <version>0.1.0</version>
    <packaging>jar</packaging>
    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.9.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
""",
        )
    )

    for _ in range(src_count):
        fname, content = _gen_java_class(industry, fkr, ai_client, package)
        files.append((f"{package_path}/{fname}", content))
        class_names.append(fname[:-5])  # strip .java

    for _ in range(test_count):
        target = random.choice(class_names) if class_names else "Placeholder"
        fname, content = _gen_java_test(package, target)
        files.append((f"{test_package_path}/{fname}", content))

    total_bytes = sum(len(c.encode("utf-8")) for _, c in files)
    return files, {
        "repo_name": repo_name,
        "language": "java",
        "file_count": len(files),
        "src_count": src_count,
        "test_count": test_count,
        "total_bytes": total_bytes,
        "package": package,
    }


# ── Top-level orchestrator ────────────────────────────────────────


def _ensure_volume(client: WorkspaceClient, warehouse_id: str, vol_fqn: str) -> None:
    execute_sql(client, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {vol_fqn}")


def _ensure_catalog_table(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    *,
    direct: bool,
) -> None:
    """Create-or-replace the catalog/direct table for code.

    Both shapes are one-row-per-FILE (not per-line like Logs) — code
    search demos work at file granularity. The direct variant adds a
    `content STRING` column with the source code inline.
    """
    if direct:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_id          STRING,
            language         STRING,
            repo_name        STRING,
            file_path        STRING,
            file_extension   STRING,
            size_bytes       BIGINT,
            line_count       BIGINT,
            content          STRING,
            generated_at     TIMESTAMP,
            metadata_json    STRING
        ) USING delta
        """
    else:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_path        STRING,
            language         STRING,
            repo_name        STRING,
            file_extension   STRING,
            size_bytes       BIGINT,
            line_count       BIGINT,
            generated_at     TIMESTAMP,
            metadata_json    STRING
        ) USING delta
        """
    execute_sql(client, warehouse_id, sql)


def _sql_str(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def generate_code(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    progress: dict | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict:
    """Top-level orchestrator. Same contract as the other unstructured
    generators.

    Output paths:
        /Volumes/<cat>/<sch>/<vol>/code/<lang>/<repo_name>/<actual-tree>

    config keys consumed:
        catalog, schema, volume, destination, types, counts, industry,
        realistic_content, faker_locale, faker_seed
    """
    progress = progress if progress is not None else {}
    stopped = stop_check or (lambda: False)

    catalog = config["catalog"]
    schema = config["schema"]
    types = config.get("types") or []
    counts = config.get("counts") or {}
    industry = config.get("industry", "healthcare")
    destination = config.get("destination", "volume_with_catalog")

    if destination not in ("volume", "volume_with_catalog", "direct_table"):
        raise ValueError(f"Unknown destination: {destination!r}")
    if not types:
        raise ValueError("'types' must contain at least one code type")
    unknown = [t for t in types if t not in CODE_TYPES]
    if unknown:
        raise ValueError(f"Unknown code types: {unknown}. Known: {sorted(CODE_TYPES)}")

    from faker import Faker

    fkr = Faker(locale=config.get("faker_locale", "en_US"))
    if config.get("faker_seed") is not None:
        fkr.seed_instance(int(config["faker_seed"]))
        random.seed(int(config["faker_seed"]))

    # AI client — opt-in via realistic_content. Reuses the shared
    # adapter that routes through Databricks Model Serving (when an
    # endpoint is picked in Settings) or the Anthropic API. Used to
    # draft function/class bodies for richer code-search embeddings.
    from src.ai_drafter import build_drafter

    ai_client = build_drafter(config, sdk_client=client)

    volume_path: str | None = None
    table_fqn: str | None = None
    if destination in ("volume", "volume_with_catalog"):
        volume = config.get("volume") or "demo_unstructured"
        vol_fqn = f"{catalog}.{schema}.{volume}"
        _ensure_volume(client, warehouse_id, vol_fqn)
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/code"

    if destination == "volume_with_catalog":
        table_fqn = f"{catalog}.{schema}.demo_code_catalog"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=False)
    elif destination == "direct_table":
        table_fqn = f"{catalog}.{schema}.demo_code"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=True)

    progress.setdefault("repos_written", 0)
    progress.setdefault("files_written", 0)
    progress.setdefault("total_bytes", 0)
    progress.setdefault("per_type", {t: 0 for t in types})
    progress.setdefault("destination", destination)

    pending_catalog_rows: list[str] = []
    pending_direct_rows: list[str] = []
    BATCH = 50

    def _flush_catalog() -> None:
        nonlocal pending_catalog_rows
        if not pending_catalog_rows or table_fqn is None:
            return
        cols = (
            "file_path",
            "language",
            "repo_name",
            "file_extension",
            "size_bytes",
            "line_count",
            "generated_at",
            "metadata_json",
        )
        sql = (
            f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES {', '.join(pending_catalog_rows)}"
        )
        execute_sql(client, warehouse_id, sql)
        pending_catalog_rows = []

    def _flush_direct() -> None:
        nonlocal pending_direct_rows
        if not pending_direct_rows or table_fqn is None:
            return
        cols = (
            "file_id",
            "language",
            "repo_name",
            "file_path",
            "file_extension",
            "size_bytes",
            "line_count",
            "content",
            "generated_at",
            "metadata_json",
        )
        sql = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES {', '.join(pending_direct_rows)}"
        execute_sql(client, warehouse_id, sql)
        pending_direct_rows = []

    started_at = datetime.now(timezone.utc)

    import io

    for type_id in types:
        if stopped():
            break
        n_repos = int(counts.get(type_id, 3))
        type_def = CODE_TYPES[type_id]
        gen_fn_name = type_def["gen_fn"]
        gen_fn = globals().get(gen_fn_name)
        if gen_fn is None:
            logger.error(f"Generator not found: {gen_fn_name}")
            continue
        language = type_def["language"]
        progress["current_type"] = type_id

        for _ in range(n_repos):
            if stopped():
                break
            try:
                files, repo_meta = gen_fn(industry, fkr, ai_client)
            except Exception as e:
                logger.error(f"  ✗ {type_id}: {e}")
                continue

            repo_name = repo_meta["repo_name"]
            for rel_path, content in files:
                content_bytes = content.encode("utf-8")
                ext = rel_path.rsplit(".", 1)[-1] if "." in rel_path else ""
                line_count = content.count("\n") + (0 if content.endswith("\n") else 1)

                current_path: str | None = None
                if volume_path is not None:
                    current_path = f"{volume_path}/{language}/{repo_name}/{rel_path}"
                    client.files.upload(
                        file_path=current_path,
                        contents=io.BytesIO(content_bytes),
                        overwrite=True,
                    )

                file_metadata_json = json.dumps(
                    {
                        "repo_name": repo_name,
                        "rel_path": rel_path,
                        "language": language,
                    },
                    default=str,
                )

                if destination == "volume_with_catalog" and current_path:
                    row = (
                        f"({_sql_str(current_path)}, "
                        f"{_sql_str(language)}, "
                        f"{_sql_str(repo_name)}, "
                        f"{_sql_str(ext)}, "
                        f"{len(content_bytes)}, "
                        f"{line_count}, "
                        f"current_timestamp(), "
                        f"{_sql_str(file_metadata_json)})"
                    )
                    pending_catalog_rows.append(row)
                    if len(pending_catalog_rows) >= BATCH:
                        _flush_catalog()
                elif destination == "direct_table":
                    file_id = uuid.uuid4().hex
                    row = (
                        f"({_sql_str(file_id)}, "
                        f"{_sql_str(language)}, "
                        f"{_sql_str(repo_name)}, "
                        f"{_sql_str(rel_path)}, "
                        f"{_sql_str(ext)}, "
                        f"{len(content_bytes)}, "
                        f"{line_count}, "
                        f"{_sql_str(content)}, "
                        f"current_timestamp(), "
                        f"{_sql_str(file_metadata_json)})"
                    )
                    pending_direct_rows.append(row)
                    if len(pending_direct_rows) >= BATCH:
                        _flush_direct()

                progress["files_written"] = progress.get("files_written", 0) + 1
                progress["total_bytes"] = progress.get("total_bytes", 0) + len(content_bytes)
                if current_path:
                    progress["current_path"] = current_path

            progress["repos_written"] = progress.get("repos_written", 0) + 1
            progress["per_type"][type_id] = progress["per_type"].get(type_id, 0) + 1

    _flush_catalog()
    _flush_direct()

    completed_at = datetime.now(timezone.utc)
    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
    from src.ai_drafter import adapter_summary

    result = {
        "status": "completed",
        "repos_written": progress["repos_written"],
        "files_written": progress["files_written"],
        "total_bytes": progress["total_bytes"],
        "per_type": progress["per_type"],
        "destination": destination,
        "volume_path": volume_path,
        "table_fqn": table_fqn,
        "duration_ms": duration_ms,
        "started_at": started_at.isoformat(timespec="seconds"),
        "completed_at": completed_at.isoformat(timespec="seconds"),
    }
    result.update(adapter_summary(ai_client))
    return result


# ── Preview ───────────────────────────────────────────────────────


def preview_code(config: dict) -> dict:
    """Estimate per-type / total counts without going near the warehouse."""
    types = config.get("types") or []
    counts = config.get("counts") or {}

    per_type = []
    total_repos = 0
    total_files = 0
    total_bytes = 0
    total_seconds = 0.0
    unknown: list[str] = []
    for t in types:
        if t not in CODE_TYPES:
            unknown.append(t)
            continue
        n_repos = int(counts.get(t, 3))
        files_per_repo = _AVG_FILES_PER_REPO.get(t, 30)
        bytes_per_file = _AVG_BYTES_PER_FILE.get(t, 1500)
        per_sec = _REPOS_PER_SECOND.get(t, 8)
        n_files = n_repos * files_per_repo
        per_type.append(
            {
                "type": t,
                "category": CODE_TYPES[t]["category"],
                "label": CODE_TYPES[t]["label"],
                "count": n_repos,
                "file_count": n_files,
                "estimated_bytes": n_files * bytes_per_file,
                "estimated_seconds": round(n_repos / per_sec, 1),
            }
        )
        total_repos += n_repos
        total_files += n_files
        total_bytes += n_files * bytes_per_file
        total_seconds += n_repos / per_sec
    return {
        "per_type": per_type,
        "total_repos": total_repos,
        "total_files": total_files,
        "total_bytes": total_bytes,
        "estimated_seconds": round(total_seconds, 1),
        "unknown_types": unknown,
    }
