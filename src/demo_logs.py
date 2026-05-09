"""Synthetic log generators for the /demo-data Logs tab.

Pairs with the other unstructured generators (documents, media,
knowledge) — same registry, orchestrator, and destination-radio
pattern. Different shape on two axes:

  - **One file = N log lines.** The "count" the operator picks per
    type is the number of *files* generated; each file holds
    ``lines_per_file`` (default 1000) log entries. So 10 nginx files
    × 1000 lines = 10,000 log lines.
  - **direct_table is one row per line, not one row per file.** Log
    analytics demos almost always want
    ``SELECT count(*) FROM demo_logs WHERE level = 'ERROR'``, not
    file-level aggregates — so the direct-table variant inserts each
    log line as its own row with timestamp + level + message +
    attributes parsed out into typed columns.

Four generators:

  - **nginx_access** — NGINX combined-log-format access lines with
    realistic IPs / paths / status codes.
  - **app_json** — structured JSON application logs
    ``{ts, level, service, trace_id, msg, attrs}``.
  - **syslog** — RFC 5424 syslog lines.
  - **otel_trace** — OpenTelemetry trace JSON: each "line" is a span
    with parent_span_id + service.name + duration_ms + attributes.

No optional Python deps — pure stdlib + Faker. Realism comes from
distributional patterns (peak-hour timestamp clustering, ~1% error
rate, common URL paths, plausible service names) rather than
narrative content, so the AI mode toggle doesn't apply.
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ── Lazy import probe ──────────────────────────────────────────────
#
# Logs has no optional Python deps — pure stdlib + Faker (which is a
# base requirement for Clone-Xs). is_available() always returns
# (True, None); the shape exists so the router code path is uniform
# with the other unstructured generators.

LOGS_AVAILABLE: bool = True
_UNAVAILABLE_REASON: str | None = None


def is_available() -> tuple[bool, str | None]:
    """Always returns (True, None) — Logs has no optional deps. Kept
    for shape-uniformity with the other unstructured generators."""
    return LOGS_AVAILABLE, _UNAVAILABLE_REASON


# ── Per-industry service registries ───────────────────────────────
#
# Each industry has a list of plausible internal-service names. The
# generator picks one per file; the file path includes the service
# name so the corpus has a coherent IA — observability demos that
# filter by service actually have meaningful subsets to filter on.

_INDUSTRY_SERVICES: dict[str, list[str]] = {
    "healthcare": [
        "patient-portal",
        "ehr-gateway",
        "billing-api",
        "scheduling-svc",
        "labs-ingest",
        "rx-pharmacy",
        "telehealth-router",
        "hl7-listener",
    ],
    "financial": [
        "payments-api",
        "fraud-scorer",
        "auth-svc",
        "ledger-writer",
        "risk-engine",
        "kyc-pipeline",
        "trade-router",
        "settlements",
    ],
    "retail": [
        "checkout-api",
        "cart-svc",
        "catalog-search",
        "inventory-sync",
        "promo-engine",
        "loyalty-svc",
        "shipping-router",
        "pos-gateway",
    ],
    "telecom": [
        "billing-api",
        "provisioning",
        "session-manager",
        "tower-monitor",
        "device-activation",
        "sms-gateway",
        "ott-router",
        "fault-correlator",
    ],
    "manufacturing": [
        "mes-bridge",
        "scada-ingest",
        "qc-scorer",
        "shift-handover",
        "yield-tracker",
        "wo-dispatcher",
        "downtime-logger",
        "andon-listener",
    ],
    "energy": [
        "grid-telemetry",
        "outage-router",
        "smart-meter-ingest",
        "demand-response",
        "ev-charger-svc",
        "scada-bridge",
        "dispatch-engine",
        "pricing-api",
    ],
    "education": [
        "lms-gateway",
        "enrollment-api",
        "grades-svc",
        "auth-sso",
        "library-search",
        "campus-doors",
        "alerts-router",
        "quiz-engine",
    ],
    "real_estate": [
        "listings-api",
        "showings-svc",
        "mortgage-router",
        "title-check",
        "tour-scheduler",
        "leads-ingest",
        "tenant-portal",
        "maintenance-api",
    ],
    "logistics": [
        "tracking-api",
        "route-optimizer",
        "fleet-telemetry",
        "yard-manager",
        "etas-engine",
        "customs-bridge",
        "claims-svc",
        "carrier-router",
    ],
    "insurance": [
        "claims-api",
        "underwriting",
        "policy-svc",
        "fnol-listener",
        "actuary-jobs",
        "agent-portal",
        "reinsurance-bridge",
        "document-ingest",
    ],
}


def _services_for(industry: str) -> list[str]:
    return _INDUSTRY_SERVICES.get(
        industry,
        ["api-gateway", "worker", "scheduler", "ingest", "exporter"],
    )


# ── Type registry ──────────────────────────────────────────────────

LOG_TYPES: dict[str, dict[str, str]] = {
    "nginx_access": {
        "category": "Access logs",
        "label": "NGINX combined-log-format access logs",
        "extension": "log",
        "gen_fn": "_gen_nginx_access",
    },
    "app_json": {
        "category": "App logs",
        "label": "Structured JSON application logs (one record per line)",
        "extension": "jsonl",
        "gen_fn": "_gen_app_json",
    },
    "syslog": {
        "category": "Syslog",
        "label": "RFC 5424 syslog lines",
        "extension": "log",
        "gen_fn": "_gen_syslog",
    },
    "otel_trace": {
        "category": "Traces",
        "label": "OpenTelemetry-shaped span JSON (one span per line)",
        "extension": "jsonl",
        "gen_fn": "_gen_otel_trace",
    },
}


# Empirical bytes-per-LINE averages (multiplied by lines_per_file in
# the preview endpoint).
_AVG_BYTES_PER_LINE: dict[str, int] = {
    "nginx_access": 220,
    "app_json": 320,
    "syslog": 180,
    "otel_trace": 380,
}

# Lines-per-second the generator can produce for each type. Used for
# the duration estimate in /preview.
_LINES_PER_SECOND: dict[str, int] = {
    "nginx_access": 50_000,
    "app_json": 30_000,
    "syslog": 60_000,
    "otel_trace": 20_000,
}


# ── Realism helpers ───────────────────────────────────────────────


def _peak_hour_weights() -> list[float]:
    """Per-hour weighting for a realistic 24-hour traffic curve.

    Two peaks (morning ~10 UTC, late afternoon ~16 UTC), low at
    night. Returns 24 floats summing to ~1.0. The orchestrator uses
    these to distribute log timestamps so the resulting files have
    plausible request-burst patterns rather than uniform noise.
    """
    return [
        0.010,
        0.008,
        0.007,
        0.006,
        0.006,
        0.008,  # 00–05
        0.015,
        0.030,
        0.055,
        0.075,
        0.085,
        0.080,  # 06–11
        0.070,
        0.065,
        0.075,
        0.085,
        0.080,
        0.060,  # 12–17
        0.050,
        0.040,
        0.030,
        0.025,
        0.018,
        0.012,  # 18–23
    ]


def _pick_timestamps(day_start: datetime, n: int) -> list[datetime]:
    """Generate `n` UTC timestamps clustered by `_peak_hour_weights`,
    returned sorted ascending so log files read as a time-ordered
    stream."""
    weights = _peak_hour_weights()
    hours = random.choices(range(24), weights=weights, k=n)
    timestamps: list[datetime] = []
    for h in hours:
        ts = day_start + timedelta(
            hours=h,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            microseconds=random.randint(0, 999_999),
        )
        timestamps.append(ts)
    timestamps.sort()
    return timestamps


def _pick_level(error_rate: float = 0.01, warn_rate: float = 0.05) -> str:
    """Pick a log level with realistic skew. Default: 1% ERROR, 5%
    WARN, ~94% INFO (with a small DEBUG slice)."""
    r = random.random()
    if r < error_rate:
        return "ERROR"
    if r < error_rate + warn_rate:
        return "WARN"
    if r < error_rate + warn_rate + 0.10:
        return "DEBUG"
    return "INFO"


_NGINX_PATHS_BY_INDUSTRY: dict[str, list[str]] = {
    "healthcare": [
        "/api/patients/{id}",
        "/api/appointments",
        "/api/labs/results",
        "/api/billing/claim/{id}",
        "/portal/login",
        "/api/rx/refill",
        "/health",
    ],
    "financial": [
        "/api/accounts/{id}/balance",
        "/api/payments",
        "/api/transactions",
        "/api/fraud/score",
        "/api/auth/token",
        "/api/cards/{id}",
        "/health",
    ],
    "retail": [
        "/api/cart",
        "/api/checkout",
        "/api/products/search",
        "/api/orders/{id}",
        "/api/inventory/sku/{id}",
        "/api/loyalty/points",
        "/health",
    ],
    "telecom": [
        "/api/subscribers/{id}",
        "/api/plans",
        "/api/billing/invoice/{id}",
        "/api/usage/{id}",
        "/api/devices/activate",
        "/health",
    ],
    "manufacturing": [
        "/api/workorders",
        "/api/equipment/{id}/status",
        "/api/qc/results",
        "/api/shifts/{id}",
        "/health",
    ],
    "energy": [
        "/api/meters/{id}/reading",
        "/api/outages",
        "/api/dispatch",
        "/api/tariffs",
        "/health",
    ],
    "education": [
        "/api/courses/{id}",
        "/api/students/{id}/grades",
        "/api/enrollments",
        "/portal/login",
        "/health",
    ],
    "real_estate": [
        "/api/listings",
        "/api/listings/{id}",
        "/api/showings",
        "/api/leads",
        "/health",
    ],
    "logistics": [
        "/api/shipments/{id}",
        "/api/routes/optimize",
        "/api/fleet/{id}/telemetry",
        "/api/eta/{id}",
        "/health",
    ],
    "insurance": [
        "/api/policies/{id}",
        "/api/claims",
        "/api/claims/{id}/status",
        "/api/quotes",
        "/health",
    ],
}


def _paths_for(industry: str) -> list[str]:
    return _NGINX_PATHS_BY_INDUSTRY.get(
        industry, ["/api/v1/resource", "/health", "/api/v1/items/{id}"]
    )


def _expand_path(template: str, fkr: Any) -> str:
    """Replace ``{id}`` placeholders with a UUID-shaped value so each
    line looks like a real request."""
    if "{id}" in template:
        return template.replace("{id}", uuid.uuid4().hex[:12])
    return template


# ── Per-type generators ───────────────────────────────────────────
#
# Each generator returns a 3-tuple:
#   (file_bytes, records, file_meta)
# where:
#   - file_bytes is what gets written to the Volume
#   - records is a list of dicts shaped for the direct_table inserts:
#       {"ts": datetime, "level": str, "message": str, "attrs": dict}
#     (one dict per line in the file)
#   - file_meta is the per-file metadata used for the catalog row


def _gen_nginx_access(
    industry: str,
    fkr: Any,
    lines_per_file: int,
    *,
    service: str,
    day_start: datetime,
) -> tuple[bytes, list[dict], dict]:
    """NGINX combined-log-format lines.

    Format (one line per request):
      $remote_addr - $remote_user [$time_local] "$request"
      $status $body_bytes_sent "$http_referer" "$http_user_agent"
    """
    paths = _paths_for(industry)
    methods_weighted = ["GET"] * 70 + ["POST"] * 20 + ["PUT"] * 5 + ["DELETE"] * 3 + ["PATCH"] * 2
    referers = [
        "-",
        "https://example.com/",
        f"https://{service}.internal/",
        "https://app.example.com/dashboard",
    ]
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        "curl/8.4.0",
        "python-requests/2.31",
        "Go-http-client/2.0",
    ]

    timestamps = _pick_timestamps(day_start, lines_per_file)

    error_count = 0
    lines: list[str] = []
    records: list[dict] = []

    for ts in timestamps:
        ip = fkr.ipv4_public()
        method = random.choice(methods_weighted)
        path = _expand_path(random.choice(paths), fkr)
        # Status distribution: ~94% 2xx, 4% 3xx, ~1% 4xx, ~1% 5xx.
        r = random.random()
        if r < 0.94:
            status = random.choice([200, 200, 200, 201, 204])
        elif r < 0.98:
            status = random.choice([301, 302, 304])
        elif r < 0.99:
            status = random.choice([400, 401, 403, 404])
        else:
            status = random.choice([500, 502, 503, 504])
        if status >= 500:
            error_count += 1
        body_bytes = random.randint(120, 50_000) if status < 400 else random.randint(0, 800)
        ua = random.choice(user_agents)
        ref = random.choice(referers)
        time_local = ts.strftime("%d/%b/%Y:%H:%M:%S +0000")

        line = (
            f'{ip} - - [{time_local}] "{method} {path} HTTP/1.1" '
            f'{status} {body_bytes} "{ref}" "{ua}"'
        )
        lines.append(line)

        # Per-line record for direct_table inserts.
        level = "ERROR" if status >= 500 else ("WARN" if status >= 400 else "INFO")
        records.append(
            {
                "ts": ts,
                "level": level,
                "message": f"{method} {path} -> {status}",
                "attrs": {
                    "remote_addr": ip,
                    "method": method,
                    "path": path,
                    "status": str(status),
                    "body_bytes": str(body_bytes),
                    "user_agent_class": "browser" if "Mozilla" in ua else "client",
                },
            }
        )

    body = "\n".join(lines) + "\n"
    bytes_out = body.encode("utf-8")
    error_rate = round(error_count / max(lines_per_file, 1), 4)
    return (
        bytes_out,
        records,
        {
            "log_type": "nginx_access",
            "service": service,
            "day": day_start.date().isoformat(),
            "line_count": len(lines),
            "error_count": error_count,
            "error_rate": error_rate,
            "format": "combined",
        },
    )


def _gen_app_json(
    industry: str,
    fkr: Any,
    lines_per_file: int,
    *,
    service: str,
    day_start: datetime,
) -> tuple[bytes, list[dict], dict]:
    """JSON Lines application logs — one record per line.

    Schema is the de-facto structured-logging shape:
      {"ts", "level", "service", "trace_id", "msg", "attrs": {...}}
    """
    timestamps = _pick_timestamps(day_start, lines_per_file)

    # Operation pool — per-request log messages reference one of these
    # to feel like real app code.
    operations = [
        "handle request",
        "validate payload",
        "fetch from cache",
        "call downstream",
        "persist to db",
        "publish event",
        "emit metric",
        "evaluate rule",
        "rate-limit check",
        "feature flag lookup",
    ]
    error_messages = [
        "downstream timeout",
        "validation failed",
        "rate limit exceeded",
        "connection reset by peer",
        "deserialization error",
        "circuit breaker open",
    ]
    warn_messages = [
        "retry attempt {n}",
        "slow query: {ms}ms",
        "cache miss for key {k}",
        "deprecation: {api}",
    ]

    error_count = 0
    lines: list[str] = []
    records: list[dict] = []

    for ts in timestamps:
        level = _pick_level(error_rate=0.01, warn_rate=0.05)
        if level == "ERROR":
            error_count += 1
            msg = random.choice(error_messages)
        elif level == "WARN":
            template = random.choice(warn_messages)
            msg = template.format(
                n=random.randint(1, 5),
                ms=random.randint(800, 4000),
                k=uuid.uuid4().hex[:8],
                api="/api/v1/legacy_endpoint",
            )
        else:
            msg = random.choice(operations)

        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]

        attrs = {
            "request_id": uuid.uuid4().hex[:12],
            "user_id": str(random.randint(1000, 99999)),
            "duration_ms": str(random.randint(2, 800)),
            "host": f"{service}-{random.randint(0, 9):02d}",
        }
        record = {
            "ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": level,
            "service": service,
            "trace_id": trace_id,
            "span_id": span_id,
            "msg": msg,
            "attrs": attrs,
        }
        lines.append(json.dumps(record, separators=(",", ":")))
        records.append(
            {
                "ts": ts,
                "level": level,
                "message": msg,
                "attrs": {**attrs, "trace_id": trace_id, "span_id": span_id},
            }
        )

    body = "\n".join(lines) + "\n"
    bytes_out = body.encode("utf-8")
    error_rate = round(error_count / max(lines_per_file, 1), 4)
    return (
        bytes_out,
        records,
        {
            "log_type": "app_json",
            "service": service,
            "day": day_start.date().isoformat(),
            "line_count": len(lines),
            "error_count": error_count,
            "error_rate": error_rate,
            "format": "json_lines",
        },
    )


# RFC 5424 syslog severity numbers.
_SYSLOG_SEVERITY = {
    "ERROR": 3,  # error
    "WARN": 4,  # warning
    "INFO": 6,  # informational
    "DEBUG": 7,  # debug
}
_SYSLOG_FACILITY = 16  # local0


def _gen_syslog(
    industry: str,
    fkr: Any,
    lines_per_file: int,
    *,
    service: str,
    day_start: datetime,
) -> tuple[bytes, list[dict], dict]:
    """RFC 5424 syslog lines.

    Format:
      <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID
      STRUCTURED-DATA MSG
    """
    timestamps = _pick_timestamps(day_start, lines_per_file)
    hostname_pool = [f"{service}-{i:02d}.internal" for i in range(8)]
    msg_pool = [
        "service started",
        "service stopped",
        "config reloaded",
        "health check passed",
        "health check failed",
        "circuit breaker tripped",
        "graceful shutdown initiated",
        "scheduled task ran",
        "queue drained",
        "leader election won",
    ]

    error_count = 0
    lines: list[str] = []
    records: list[dict] = []

    for ts in timestamps:
        level = _pick_level(error_rate=0.01, warn_rate=0.04)
        sev = _SYSLOG_SEVERITY[level]
        pri = _SYSLOG_FACILITY * 8 + sev
        if level == "ERROR":
            error_count += 1
            msg = "health check failed"
        else:
            msg = random.choice(msg_pool)

        host = random.choice(hostname_pool)
        procid = random.randint(1000, 65535)
        msgid = "ID" + str(random.randint(1, 999))
        ts_str = ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        # Structured data block — one element with a couple of params.
        # Keeps the line shape close to real-world parsers
        # (e.g. rsyslog's RFC5424 emitter).
        sd = f'[exampleSDID@32473 iut="3" eventSource="{service}" eventID="{msgid}"]'

        line = f"<{pri}>1 {ts_str} {host} {service} {procid} {msgid} {sd} {msg}"
        lines.append(line)
        records.append(
            {
                "ts": ts,
                "level": level,
                "message": msg,
                "attrs": {
                    "hostname": host,
                    "procid": str(procid),
                    "msgid": msgid,
                    "facility": str(_SYSLOG_FACILITY),
                    "severity": str(sev),
                },
            }
        )

    body = "\n".join(lines) + "\n"
    bytes_out = body.encode("utf-8")
    error_rate = round(error_count / max(lines_per_file, 1), 4)
    return (
        bytes_out,
        records,
        {
            "log_type": "syslog",
            "service": service,
            "day": day_start.date().isoformat(),
            "line_count": len(lines),
            "error_count": error_count,
            "error_rate": error_rate,
            "format": "rfc5424",
        },
    )


def _gen_otel_trace(
    industry: str,
    fkr: Any,
    lines_per_file: int,
    *,
    service: str,
    day_start: datetime,
) -> tuple[bytes, list[dict], dict]:
    """OpenTelemetry-shaped span JSON, one span per line.

    Each line is a span dict with parent_span_id wired up so spans
    form trees of 3-8 spans per trace_id. The OTel collector's JSON
    encoding is the closest to this shape — close enough that
    standard OTel SQL views (Lakehouse Monitoring, etc.) work
    against the result without re-shaping.
    """
    span_kinds = ["SERVER", "CLIENT", "INTERNAL", "PRODUCER", "CONSUMER"]
    op_pool = [
        "http.server.request",
        "http.client.request",
        "db.query",
        "kafka.publish",
        "cache.lookup",
        "task.execute",
        "rpc.call",
    ]
    downstream_services = _services_for(industry)

    error_count = 0
    lines: list[str] = []
    records: list[dict] = []

    spans_emitted = 0
    while spans_emitted < lines_per_file:
        # Build one trace tree of 3–8 spans, pick a base timestamp
        # for the trace, then stagger child span starts.
        trace_id = uuid.uuid4().hex
        spans_in_trace = min(random.randint(3, 8), lines_per_file - spans_emitted)
        # Pick the base timestamp from the day's distribution for
        # this single trace; child spans fall within +0–500ms.
        base_hour_weights = _peak_hour_weights()
        h = random.choices(range(24), weights=base_hour_weights, k=1)[0]
        base_ts = day_start + timedelta(
            hours=h,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            microseconds=random.randint(0, 999_999),
        )
        root_span_id = uuid.uuid4().hex[:16]
        parent_for_next = root_span_id
        for i in range(spans_in_trace):
            span_id = root_span_id if i == 0 else uuid.uuid4().hex[:16]
            parent_id = "" if i == 0 else parent_for_next
            kind = "SERVER" if i == 0 else random.choice(span_kinds)
            op = random.choice(op_pool)
            duration_ms = random.randint(2, 1200)
            ts = base_ts + timedelta(milliseconds=random.randint(0, 500) * i)

            # Status: ERROR if status_code is set; otherwise OK.
            r = random.random()
            if r < 0.012:
                status_code = "ERROR"
                level = "ERROR"
                error_count += 1
            elif r < 0.05:
                status_code = "OK"
                level = "WARN"
            else:
                status_code = "OK"
                level = "INFO"

            attrs = {
                "service.name": service if i == 0 else random.choice(downstream_services),
                "operation": op,
                "span.kind": kind,
                "duration_ms": str(duration_ms),
                "status_code": status_code,
            }

            span = {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_id,
                "name": op,
                "kind": kind,
                "service": attrs["service.name"],
                "start_time": ts.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "duration_ms": duration_ms,
                "status": {"code": status_code},
                "attributes": attrs,
            }
            lines.append(json.dumps(span, separators=(",", ":")))
            records.append(
                {
                    "ts": ts,
                    "level": level,
                    "message": f"{op} ({duration_ms}ms)",
                    "attrs": {
                        "trace_id": trace_id,
                        "span_id": span_id,
                        "parent_span_id": parent_id or "",
                        **attrs,
                    },
                }
            )
            spans_emitted += 1
            if spans_emitted >= lines_per_file:
                break
            # Random tree shape: sometimes child of root, sometimes
            # child of previous span.
            if random.random() < 0.6:
                parent_for_next = span_id

    body = "\n".join(lines) + "\n"
    bytes_out = body.encode("utf-8")
    error_rate = round(error_count / max(lines_per_file, 1), 4)
    return (
        bytes_out,
        records,
        {
            "log_type": "otel_trace",
            "service": service,
            "day": day_start.date().isoformat(),
            "line_count": len(lines),
            "error_count": error_count,
            "error_rate": error_rate,
            "format": "otel_json",
        },
    )


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
    """Create-or-replace the catalog/direct table for logs.

    Logs has two distinct table shapes:

    - **catalog (one row per FILE)** — mirrors the documents/media/
      knowledge per-file catalogs. file_path + per-file metadata.
    - **direct (one row per LINE)** — the natural shape for log
      analytics. Operators query
      ``SELECT count(*) FROM demo_logs WHERE level = 'ERROR'``,
      not file-level aggregates.
    """
    if direct:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            log_id          STRING,
            log_type        STRING,
            service         STRING,
            ts              TIMESTAMP,
            level           STRING,
            message         STRING,
            attrs           MAP<STRING, STRING>,
            generated_at    TIMESTAMP
        ) USING delta
        """
    else:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_path       STRING,
            log_type        STRING,
            service         STRING,
            day             STRING,
            size_bytes      BIGINT,
            line_count      BIGINT,
            error_count     BIGINT,
            error_rate      DOUBLE,
            generated_at    TIMESTAMP,
            metadata_json   STRING
        ) USING delta
        """
    execute_sql(client, warehouse_id, sql)


def _sql_str(s: str | None) -> str:
    """Single-quote escape for inline INSERT VALUES."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _ts_literal(ts: datetime) -> str:
    """Render a Python datetime as a Spark TIMESTAMP literal.

    Uses millisecond precision — Spark's TIMESTAMP type stores
    microseconds but accepts millisecond strings, and dropping
    sub-ms detail keeps the INSERT VALUES strings short.
    """
    return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"


def _attrs_literal(attrs: dict[str, str]) -> str:
    """Render a Python dict as a Spark MAP<STRING,STRING> literal:
    ``map('k1','v1','k2','v2', ...)``.

    All keys/values are coerced to strings to match the column type;
    callers should pass string-shaped attrs."""
    if not attrs:
        return "map()"
    parts: list[str] = []
    for k, v in attrs.items():
        parts.append(_sql_str(str(k)))
        parts.append(_sql_str(str(v)))
    return f"map({', '.join(parts)})"


def generate_logs(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    progress: dict | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict:
    """Top-level orchestrator. Same contract as the other unstructured
    generators (``generate_documents`` / ``generate_media`` /
    ``generate_knowledge``).

    Output paths:
        /Volumes/<cat>/<sch>/<vol>/logs/<type>/<service>/<day>/<file>.<ext>

    config keys consumed:
        catalog, schema, volume, destination, types, counts, industry,
        lines_per_file (default 1000),
        days_back (default 7 — files spread across last N UTC days),
        faker_locale, faker_seed
    """
    progress = progress if progress is not None else {}
    stopped = stop_check or (lambda: False)

    catalog = config["catalog"]
    schema = config["schema"]
    types = config.get("types") or []
    counts = config.get("counts") or {}
    industry = config.get("industry", "healthcare")
    destination = config.get("destination", "volume_with_catalog")
    lines_per_file = int(config.get("lines_per_file", 1000))
    days_back = int(config.get("days_back", 7))

    if destination not in ("volume", "volume_with_catalog", "direct_table"):
        raise ValueError(f"Unknown destination: {destination!r}")
    if not types:
        raise ValueError("'types' must contain at least one log type")
    unknown = [t for t in types if t not in LOG_TYPES]
    if unknown:
        raise ValueError(f"Unknown log types: {unknown}. Known: {sorted(LOG_TYPES)}")
    if lines_per_file <= 0:
        raise ValueError(f"lines_per_file must be > 0 (got {lines_per_file})")

    from faker import Faker

    fkr = Faker(locale=config.get("faker_locale", "en_US"))
    if config.get("faker_seed") is not None:
        fkr.seed_instance(int(config["faker_seed"]))
        random.seed(int(config["faker_seed"]))

    services = _services_for(industry)

    volume_path: str | None = None
    table_fqn: str | None = None
    if destination in ("volume", "volume_with_catalog"):
        volume = config.get("volume") or "demo_unstructured"
        vol_fqn = f"{catalog}.{schema}.{volume}"
        _ensure_volume(client, warehouse_id, vol_fqn)
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/logs"

    if destination == "volume_with_catalog":
        table_fqn = f"{catalog}.{schema}.demo_logs_catalog"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=False)
    elif destination == "direct_table":
        table_fqn = f"{catalog}.{schema}.demo_logs"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=True)

    progress.setdefault("files_written", 0)
    progress.setdefault("lines_written", 0)
    progress.setdefault("total_bytes", 0)
    progress.setdefault("per_type", {t: 0 for t in types})
    progress.setdefault("destination", destination)

    pending_catalog_rows: list[str] = []
    pending_direct_rows: list[str] = []
    CATALOG_BATCH = 50
    # Direct-table batches are smaller because each row is a LINE not
    # a file — 1000 lines per file × 50 = 50,000 row inserts which is
    # too big for one INSERT VALUES. Cap at 500 lines per insert.
    DIRECT_BATCH = 500

    def _flush_catalog() -> None:
        nonlocal pending_catalog_rows
        if not pending_catalog_rows or table_fqn is None:
            return
        cols = (
            "file_path",
            "log_type",
            "service",
            "day",
            "size_bytes",
            "line_count",
            "error_count",
            "error_rate",
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
            "log_id",
            "log_type",
            "service",
            "ts",
            "level",
            "message",
            "attrs",
            "generated_at",
        )
        sql = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES {', '.join(pending_direct_rows)}"
        execute_sql(client, warehouse_id, sql)
        pending_direct_rows = []

    started_at = datetime.now(timezone.utc)

    import io

    # Today's UTC date floored to midnight — files are distributed
    # across `days_back` recent days so the corpus has a realistic
    # multi-day shape.
    today_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    for type_id in types:
        if stopped():
            break
        n_files = int(counts.get(type_id, 5))
        type_def = LOG_TYPES[type_id]
        gen_fn_name = type_def["gen_fn"]
        gen_fn = globals().get(gen_fn_name)
        if gen_fn is None:
            logger.error(f"Generator not found: {gen_fn_name}")
            continue
        progress["current_type"] = type_id

        for seq in range(n_files):
            if stopped():
                break
            service = random.choice(services)
            day_offset = random.randint(0, max(days_back - 1, 0))
            day_start = today_midnight - timedelta(days=day_offset)

            try:
                file_bytes, records, file_meta = gen_fn(
                    industry,
                    fkr,
                    lines_per_file,
                    service=service,
                    day_start=day_start,
                )
            except Exception as e:
                logger.error(f"  ✗ {type_id} #{seq}: {e}")
                continue

            file_id = uuid.uuid4().hex
            ext = type_def["extension"]
            file_name = f"{type_id}_{file_id}.{ext}"
            day_str = file_meta["day"]

            current_path: str | None = None
            if volume_path is not None:
                current_path = f"{volume_path}/{type_id}/{service}/{day_str}/{file_name}"
                client.files.upload(
                    file_path=current_path,
                    contents=io.BytesIO(file_bytes),
                    overwrite=True,
                )

            metadata_json = json.dumps(file_meta, default=str)

            if destination == "volume_with_catalog" and current_path:
                row = (
                    f"({_sql_str(current_path)}, "
                    f"{_sql_str(type_id)}, "
                    f"{_sql_str(service)}, "
                    f"{_sql_str(day_str)}, "
                    f"{len(file_bytes)}, "
                    f"{file_meta['line_count']}, "
                    f"{file_meta['error_count']}, "
                    f"{file_meta['error_rate']}, "
                    f"current_timestamp(), "
                    f"{_sql_str(metadata_json)})"
                )
                pending_catalog_rows.append(row)
                if len(pending_catalog_rows) >= CATALOG_BATCH:
                    _flush_catalog()
            elif destination == "direct_table":
                # One row per LINE, batched in DIRECT_BATCH chunks.
                for rec in records:
                    log_id = uuid.uuid4().hex
                    row = (
                        f"({_sql_str(log_id)}, "
                        f"{_sql_str(type_id)}, "
                        f"{_sql_str(service)}, "
                        f"{_ts_literal(rec['ts'])}, "
                        f"{_sql_str(rec['level'])}, "
                        f"{_sql_str(rec['message'])}, "
                        f"{_attrs_literal(rec['attrs'])}, "
                        f"current_timestamp())"
                    )
                    pending_direct_rows.append(row)
                    if len(pending_direct_rows) >= DIRECT_BATCH:
                        _flush_direct()

            progress["files_written"] = progress.get("files_written", 0) + 1
            progress["lines_written"] = progress.get("lines_written", 0) + len(records)
            progress["total_bytes"] = progress.get("total_bytes", 0) + len(file_bytes)
            progress["per_type"][type_id] = progress["per_type"].get(type_id, 0) + 1
            if current_path:
                progress["current_path"] = current_path

    _flush_catalog()
    _flush_direct()

    completed_at = datetime.now(timezone.utc)
    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
    return {
        "status": "completed",
        "files_written": progress["files_written"],
        "lines_written": progress["lines_written"],
        "total_bytes": progress["total_bytes"],
        "per_type": progress["per_type"],
        "destination": destination,
        "volume_path": volume_path,
        "table_fqn": table_fqn,
        "duration_ms": duration_ms,
        "started_at": started_at.isoformat(timespec="seconds"),
        "completed_at": completed_at.isoformat(timespec="seconds"),
    }


# ── Preview (pure arithmetic) ─────────────────────────────────────


def preview_logs(config: dict) -> dict:
    """Return per-type / total estimates without going near the warehouse.

    The "count" the operator picks is the number of files; total
    lines = files × lines_per_file, total bytes = lines × per-line
    average.
    """
    types = config.get("types") or []
    counts = config.get("counts") or {}
    lines_per_file = int(config.get("lines_per_file", 1000))

    per_type = []
    total_files = 0
    total_lines = 0
    total_bytes = 0
    total_seconds = 0.0
    unknown: list[str] = []
    for t in types:
        if t not in LOG_TYPES:
            unknown.append(t)
            continue
        n_files = int(counts.get(t, 5))
        line_bytes = _AVG_BYTES_PER_LINE.get(t, 250)
        per_sec = _LINES_PER_SECOND.get(t, 30_000)
        n_lines = n_files * lines_per_file
        per_type.append(
            {
                "type": t,
                "category": LOG_TYPES[t]["category"],
                "label": LOG_TYPES[t]["label"],
                "count": n_files,
                "line_count": n_lines,
                "estimated_bytes": n_lines * line_bytes,
                "estimated_seconds": round(n_lines / per_sec, 1),
            }
        )
        total_files += n_files
        total_lines += n_lines
        total_bytes += n_lines * line_bytes
        total_seconds += n_lines / per_sec
    return {
        "per_type": per_type,
        "total_files": total_files,
        "total_lines": total_lines,
        "total_bytes": total_bytes,
        "estimated_seconds": round(total_seconds, 1),
        "unknown_types": unknown,
    }
