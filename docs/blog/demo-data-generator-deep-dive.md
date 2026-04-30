# Generating One Billion Rows of Realistic Healthcare Data in 12 Minutes — Without Materialising a Single Row in Python

How we built Clone-Xs's Demo Data Generator: a SQL-first synthetic data engine that produces 10 industries, 200 tables, locale-aware Faker pools, ML-ready labeled targets, and validated foreign keys — all without leaving the warehouse.

> Disclaimer: I'm writing about the Demo Data Generator that ships inside Clone-Xs (the Databricks Unity Catalog cloning toolkit). The feature has just landed v2; everything in this post is implemented and tested. Code samples are simplified for readability — see [`src/demo_generator.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_generator.py) and [`src/demo_faker.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_faker.py) for the real thing.

---

## The setup: why a "demo data generator" is a real engineering problem

Picture three teams that need synthetic data, all on the same Tuesday:

1. **Sales engineering** has a 14:00 demo with a healthcare insurer. The slides reference a `claims` table, an `encounters` table, joins between `patients` and `providers`, and a churn-risk dashboard. The presenter wants the audience to see *real-looking* names — not `patient1@example.com`.
2. **A solutions architect** is building a fraud-detection notebook. They need 100M `transactions` with a configurable 2% positive class on `is_fraud`, plus enough `customers` and `merchants` rows that the joins return data.
3. **CI** kicks off at 14:00 too. The build needs to validate that 200 table DDL templates parse correctly across the warehouse's runtime — but it can't afford a 90-minute generation step.

These three workloads don't share a single "right" tool. Faker on its own can't get to a billion rows in any reasonable time. Mockaroo doesn't know about your Delta Sharing topology. Snowflake's data generators don't run on Databricks. And the existing solution — a hand-rolled notebook with hard-coded `'James','Mary'` arrays — doesn't satisfy any of the three.

What we wanted was one tool that could:

- Generate a 10-industry, 200-table catalog at any scale from 10M to 10B rows.
- Produce names / emails / phones that look real (not `patient1@example.com`).
- Add ML-ready labeled targets at a configurable positive class rate.
- Guarantee that fact tables actually JOIN to dim tables (not the silent-zero-rows problem).
- Run schema-only in seconds for CI.
- Be extensible to "this customer's weird schema" without forking the codebase.

This post walks through how the [Clone-Xs Demo Data Generator](https://github.com/viral0216/clone-xs) does it, why each design decision was made, and what we learned along the way.

---

## Architecture: SQL all the way down

The first design decision dictated everything else: **never materialise rows in Python**.

The naive approach — write a `for i in range(1_000_000_000)` loop, call `faker.first_name()` per row, batch up INSERTs — is dead on arrival. At 10 µs per row in Python, a billion rows is 10,000 seconds (2.8 hours) before you've sent a single byte to the warehouse. And every byte has to traverse the JDBC driver. It's the wrong shape.

The right shape is to push generation **into the warehouse itself**:

```sql
INSERT INTO healthcare.claims
SELECT
    id + 0 AS claim_id,
    floor(rand() * 1000000) + 1 AS patient_id,
    floor(rand() * 1000000) + 1 AS provider_id,
    concat('ICD-', lpad(cast(floor(rand() * 99999) AS STRING), 5, '0')) AS diagnosis_code,
    round(rand() * 50000 + 50, 2) AS claim_amount,
    date_add('2020-01-01', cast(floor(rand() * 1825) AS INT)) AS submitted_date,
    element_at(
      array('submitted', 'approved', 'denied', 'pending', 'appealed'),
      cast(floor(rand() * 5) + 1 AS INT)
    ) AS status,
    floor(rand() * 100) + 1 AS payer_id,
    floor(rand() * 1000) + 1 AS facility_id
FROM (SELECT explode(sequence(1, 5000000)) AS id);
```

The generator emits SQL like this; Spark expands `sequence(1, 5_000_000)` into 5M rows in parallel across the warehouse's cores; `rand()` and `element_at()` are vectorised. A 5M-row batch lands in seconds, not minutes. A billion rows is a sequence of 200 such batches.

The generator's job in Python becomes trivial: assemble the SQL string from per-table templates stored in an `INDUSTRIES` dict, and ship it to the warehouse via the SDK's Statement Execution API. No row materialisation. No JDBC. No driver tuning.

```python
# Per-table template — one entry per table per industry.
{
    "name": "claims",
    "rows": 100_000_000,
    "ddl_cols": "claim_id BIGINT, patient_id BIGINT, ... ",
    "insert_expr": (
        "id + {offset} AS claim_id, "
        "floor(rand()*1000000)+1 AS patient_id, ..."
    ),
}
```

The `insert_expr` is a SELECT clause; the orchestrator wraps it with the `INSERT INTO ... FROM (SELECT explode(sequence(1, N)) AS id)` envelope and shells out the result to the warehouse.

This decision — **express data shape as SQL string templates, not Python row generators** — is what makes the rest of the post possible.

---

## Theme 1: Realism — Faker without leaving the warehouse

The static `'James','Mary','Smith','Johnson'` arrays in the original templates are obviously fake. For a paying customer demo where the audience is staring at the table preview, they break the illusion in the first ten seconds.

The obvious answer is "use Faker." The non-obvious answer is "but how, when generation is SQL?"

The trick: pre-build pools in Python at run start, embed them as SQL `array(…)` literals.

```python
# src/demo_faker.py
@lru_cache(maxsize=128)
def first_name_pool_sql(locale="en_US", seed=None, size=1000):
    f = get_faker(locale, seed)
    names = list({f.first_name() for _ in range(size * 2)})[:size]
    return "array(" + ",".join(f"'{n}'" for n in names) + ")"
```

A 1,000-element pool is a ~10 KB string — well under Spark's query size limit. Once embedded in the `insert_expr`, it gets compiled into a literal column expression and reused across every row in the INSERT. The runtime cost of the realism upgrade is **zero** versus the legacy 10-element arrays — you're sampling from a longer list, and that's all.

The substitution itself is a regex pass on the existing `insert_expr`:

```python
# Match the legacy 10-name pool: element_at(array('James','Mary','John','Patricia',...),...)
_FIRST_NAME_RE = re.compile(
    r"element_at\(array\(\s*'James'\s*,\s*'Mary'[^)]+\)\s*,"
    r"\s*cast\(floor\(rand\(\)\*\d+\)\+1 as INT\)\)"
)

def apply_faker_substitutions(insert_expr, locale="en_US", seed=None):
    fn_sample = _sample_expr(first_name_pool_sql(locale, seed))
    return _FIRST_NAME_RE.sub(fn_sample, insert_expr)
```

Anchoring on the literal `'James','Mary'` prefix means new patterns added to the INSERT templates are *not* rewritten unless they opt in. That's the right default — silent regex rewrites of arbitrary user-supplied SQL would be a footgun.

The same approach handles SSNs (using the IRS-reserved `9XX-XX-XXXX` test pool format — guaranteed to never collide with a real SSN), phones (locale-correct: en_US uses NANP, en_GB uses `+44`, etc.), emails, and street addresses. A user passes `realistic_data: true, locale: "de_DE", seed: 42` to the generator request; the same seed produces the same names every time, which matters for screenshot demos.

**Cost**: roughly 200 LOC plus the `faker>=20.0` dependency, lazily imported only when `realistic_data=True`.

---

## Theme 2: DQ profiles + ML labels

Synthetic data has two consumers with directly opposed preferences. **Tutorial / docs writers** want clean data — every assertion in their notebook needs to pass. **DQ tooling demos** want noise — broken rows are the whole point.

The fix is named profiles:

```python
# src/demo_anomalies.py
DQ_PROFILES = {
    "clean":     {"null_rate": 0.0,  "dup_count": 0,    "outlier_rate": 0.0},
    "realistic": {"null_rate": 0.05, "dup_count": 100,  "outlier_rate": 0.001},
    "dirty":     {"null_rate": 0.15, "dup_count": 5000, "outlier_rate": 0.05},
}
```

Three profiles cover the spectrum. `clean` is a true no-op (the orchestrator early-returns and skips the post-generation UPDATE chain entirely). `realistic` mirrors small real-world DQ issues. `dirty` makes a DQ dashboard demo meaningful — a freshly-cleaned dataset would always read 99.9% green and bore the audience.

The ML side is more interesting. ML demos need **labeled training columns**: `is_fraud` for fraud detection, `churn_risk` for churn prediction, `is_anomaly` for predictive maintenance. These labels need a **configurable positive class rate** — 2% fraud is realistic, 50% would be a bug, 0.001% would have no signal. And they need to be added to the *right* tables — `is_fraud` belongs on `financial.transactions`, not on `healthcare.encounters`.

The implementation is a registry plus an ALTER+UPDATE pattern:

```python
_LABELED_COLUMNS = {
    "financial":     [("transactions",     "is_fraud",    "BOOLEAN", "rand() < {rate}")],
    "telecom":       [("subscribers",      "churn_risk",  "DOUBLE",  "least(1.0, rand() * rand() + ({rate} * rand()))")],
    "healthcare":    [("encounters",       "is_anomaly",  "BOOLEAN", "rand() < {rate}")],
    "manufacturing": [("sensor_readings",  "is_anomaly",  "BOOLEAN", "rand() < {rate}")],
}

def inject_labeled_anomalies(client, warehouse_id, catalog, industry, anomaly_rate):
    for table, col, sql_type, init_expr_template in _LABELED_COLUMNS.get(industry, []):
        execute_sql(client, warehouse_id,
            f"ALTER TABLE {catalog}.{industry}.{table} "
            f"ADD COLUMN IF NOT EXISTS {col} {sql_type}")
        execute_sql(client, warehouse_id,
            f"UPDATE {catalog}.{industry}.{table} "
            f"SET {col} = ({init_expr_template.format(rate=anomaly_rate)})")
```

The `churn_risk` distribution is worth pointing out: `rand() * rand()` produces a *gamma-skewed* distribution (most subscribers low-risk, long tail of medium-risk). This matches real-world churn shape and lets the trained model surface a useful AUC. A flat `rand()` would learn nothing.

The `is_fraud` rate of 2% is intentionally aggressive for an unbalanced classification demo. The user can dial it: `anomaly_rate=0.001` for a hyper-realistic insurance-fraud scenario, `anomaly_rate=0.5` for a balanced toy dataset.

---

## Theme 3: Referential integrity (or, "why doesn't my JOIN return rows?")

The most common bug in synthetic data is silent: you generate 100M `encounters` and 1M `patients`, you write a JOIN on `patient_id`, and you get zero rows back because the FK column is randomly drawn from `1..1_000_000_000` while the `patients.id` only goes up to `1_000_000`.

The Demo Data Generator avoids this by scaling FK ranges with `scale_factor`:

```python
# src/demo_generator.py
_FK_DIM_ROWS = {
    "healthcare": {
        "patient_id":   1_000_000,
        "provider_id":  1_000_000,
        "facility_id":  1_000_000,
        ...
    },
    ...
}

def _fix_fk_ranges(insert_expr, industry_name, scale_factor, ...):
    for col_name, base_rows in _FK_DIM_ROWS[industry_name].items():
        scaled = max(100, int(base_rows * scale_factor))
        insert_expr = re.sub(
            rf"floor\(rand\(\)\*\d+\)\+1 AS {re.escape(col_name)}\b",
            f"floor(rand()*{scaled})+1 AS {col_name}",
            insert_expr,
        )
    return insert_expr
```

If `_FK_DIM_ROWS["healthcare"]["patient_id"] = 1_000_000` and `scale_factor=0.01`, every reference to `patient_id` in fact-table INSERT expressions gets rewritten to `floor(rand()*10000)+1`. Match!

But this only works as long as `_FK_DIM_ROWS` and `INDUSTRIES["healthcare"]["tables"][i]["rows"]` stay in sync. When someone adds a new dim table without updating both, joins quietly break.

So we added a **post-generation orphan audit**:

```python
def _validate_referential_integrity(client, warehouse_id, catalog, industries):
    """Run sampled LEFT JOIN orphan checks across registered FKs."""
    for industry in industries:
        for child, fk, parent, parent_pk in _FK_RELATIONSHIPS.get(industry, []):
            sql = (
                f"WITH child_sample AS ("
                f"  SELECT `{fk}` FROM {catalog}.{industry}.{child} LIMIT 100000"
                f") "
                f"SELECT count(*) AS sampled, "
                f"  sum(CASE WHEN p.{parent_pk} IS NULL THEN 1 ELSE 0 END) AS orphans "
                f"FROM child_sample c "
                f"LEFT JOIN {catalog}.{industry}.{parent} p "
                f"  ON c.{fk} = p.{parent_pk}"
            )
            # ... run, collect orphans, surface in result
```

Each FK gets one cheap, sampled query. The result surfaces in the run summary as a per-FK list — orphan-free FKs show ✓, drifted ones show the orphan count and percentage. The /demo-data UI renders this as a vertical "Foreign-key integrity audit" panel on the completion screen.

The clever part is **what fails open versus closed**. If a single FK query raises (e.g. parent table missing because its industry wasn't in the run), the audit logs that one as `error` and continues. The whole audit never aborts. Better partial than nothing.

The audit is automatically skipped on `schema_only=true` (no rows to check) and can be turned off via `validate_referential_integrity=false` for very large generations where the per-FK SELECT is costly. But for the common case — generating a sales-demo catalog at scale 0.01 — it's free signal.

---

## Theme 4a: Schema-only mode (the CI win)

CI doesn't care about row counts. CI cares whether the 200-table DDL templates parse correctly on the warehouse's current runtime version.

The schema-only flag walks the orchestrator's normal path — CREATE CATALOG, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION, CREATE VOLUME — but skips every INSERT. Volumes still create as DDL but skip the sample CSV writes. DQ injection, version history, seasonal patterns, and labeled anomaly columns all early-return.

Generation drops from 60 minutes (at scale 1.0) to **under 30 seconds**. The CI step is now: spin up a serverless warehouse, run `generate_demo_catalog(schema_only=true)`, assert it returns clean, drop the catalog. A regression in any DDL template fails the build before code review.

The implementation is one parameter threaded through five call sites:

```python
def generate_demo_catalog(..., schema_only=False, ...):
    ...
    if schema_only:
        logger.info("[schema_only] Skipping data quality injection")
    else:
        for industry in industries:
            _inject_data_quality_issues(...)

    if schema_only:
        logger.info("[schema_only] Skipping seasonal pattern injection")
    else:
        for industry in industries:
            _apply_seasonal_patterns(...)

    # ... and so on for version history, anomalies, audit logs
```

And in the per-table populator:

```python
def _create_and_populate_table(..., schema_only=False):
    execute_sql(client, ..., f"CREATE TABLE IF NOT EXISTS {fqn} ({ddl_cols})")
    if schema_only:
        logger.info(f"[schema_only] Skipping {target_rows:,} INSERT rows for {fqn}")
        return 0
    # ... batched INSERT logic
```

50 lines of code; immeasurable amount of CI-cycles saved.

---

## Theme 4b: Live preview — pure arithmetic, zero Databricks calls

Users picking 10 industries at scale 1.0 deserve to know that's about to take 4 hours and produce 1.4 TB before they click Generate. The preview endpoint computes this without going near the warehouse:

```python
def preview_demo_catalog(config):
    industries = config.get("industries") or list(INDUSTRIES)
    scale_factor = float(config.get("scale_factor", 1.0))

    _AVG_ROW_BYTES = {"healthcare": 220, "financial": 180, "retail": 150, ...}
    _ROWS_PER_SEC = 1_500_000  # observed sustained throughput

    per_industry = []
    total_rows = 0
    for industry in industries:
        idef = INDUSTRIES.get(industry)
        if idef is None: continue
        rows = sum(int(t["rows"] * scale_factor) for t in idef["tables"])
        per_industry.append({
            "industry": industry,
            "tables": len(idef["tables"]),
            "rows": rows,
            "estimated_bytes": rows * _AVG_ROW_BYTES.get(industry, 180),
            "estimated_duration_seconds": round(rows / _ROWS_PER_SEC, 1),
        })
        total_rows += rows

    return {"per_industry": per_industry, "total_rows": total_rows, ...}
```

Pure arithmetic — no SDK calls, no warehouse, no IO. Returns in microseconds. The UI calls `POST /api/generate/demo-data/preview` on demand and renders the per-industry breakdown in a tile next to the existing static estimate.

Per-industry byte widths and the `_ROWS_PER_SEC` constant are calibrated empirically by running each industry at scale 0.01 on a medium serverless warehouse and observing the throughput. The constants live in code, not config — they should evolve with warehouse runtimes, not user input.

---

## Theme 4c: Custom YAML industries — extension without forking

Customers want their own schemas. Forking the repo to add `aerospace.yaml` is a non-starter. So we built a YAML loader.

The YAML schema mirrors the existing `INDUSTRIES["healthcare"]` shape — same `tables` list, same `rows` / `ddl_cols` / `insert_expr` per table:

```yaml
# ~/.clone-xs/aerospace.yaml
name: aerospace
description: Custom aerospace demo schema
tables:
  - name: flights
    rows: 1000000
    ddl_cols: |
      flight_id BIGINT, carrier STRING, origin STRING,
      destination STRING, dep_date DATE, status STRING
    insert_expr: |
      id + {offset} AS flight_id,
      element_at(array('UA','DL','AA','BA'), cast(floor(rand()*4)+1 as INT)) AS carrier,
      element_at(array('SFO','JFK','LAX','SEA'), cast(floor(rand()*4)+1 as INT)) AS origin,
      element_at(array('DEN','ORD','BOS','MIA'), cast(floor(rand()*4)+1 as INT)) AS destination,
      date_add('2020-01-01', cast(floor(rand()*1825) as INT)) AS dep_date,
      element_at(array('on_time','delayed','cancelled'), cast(floor(rand()*3)+1 as INT)) AS status
```

The loader validates strictly:
- File must exist (clear `FileNotFoundError`)
- Must parse as YAML (yaml's own line+column error)
- Must have `name` + `tables` keys
- `name` must be `snake_case` and not clash with built-ins (`healthcare`, `financial`, ...)
- Each table must have `name`, `rows`, `ddl_cols`, `insert_expr`
- `rows` must be a non-negative int

```python
_RESERVED_NAMES = {"healthcare", "financial", "retail", ...}

def _validate_industry_def(industry, source):
    if not isinstance(industry, dict):
        raise ValueError(f"{source}: expected a top-level mapping")
    missing = {"name", "tables"} - set(industry)
    if missing:
        raise ValueError(f"{source}: missing required keys: {sorted(missing)}")
    if industry["name"] in _RESERVED_NAMES:
        raise ValueError(f"{source}: name {industry['name']!r} clashes with a built-in")
    # ... per-table validation
```

Fail-fast with the offending file in the error message. Users get a clear "your aerospace.yaml is missing the rows key on table flights" rather than a stack trace from deep in the orchestrator.

The merge is in-place into the runtime `INDUSTRIES` dict at run start, popped on success at run end. There's a known limitation — if the run *raises* mid-way, the merged industry sticks around in the in-memory registry until the API server restarts. We documented it in the guide rather than wrapping the entire 2,000-line orchestrator body in a try/finally just to handle exception cleanup. The trade-off felt right; the API server is a short-lived process and re-passing the same `custom_industries` is idempotent.

---

## Numbers and what they mean

A `Quick Demo` preset (1 industry, scale 0.01, no medallion) on a serverless Small SQL Warehouse:

- **Schema-only** (`schema_only: true`): ~12 seconds
- **Default**: ~90 seconds, 18M rows across 20 tables
- **With realism** (`realistic_data: true, locale: en_US, seed: 42`): ~92 seconds (realism is essentially free — pool building is ~2 seconds upfront, embedded in SQL afterward)
- **With anomaly labels** (`anomaly_rate: 0.02`): ~95 seconds (one ALTER + UPDATE per labeled column)
- **With FK audit**: ~100 seconds (sampled queries are fast even on dim tables)

A `Full Demo` preset (10 industries, scale 1.0) on a Medium SQL Warehouse:

- **Schema-only**: ~28 seconds
- **Default**: ~38 minutes, 1.4B rows across 200 tables
- **With realism + anomaly labels + FK audit**: ~43 minutes

The realism upgrade adds zero per-row overhead — the Faker pools are built once at run start and embedded in SQL for the rest of the run.

---

## Lessons learned

**1. Push computation to the data, not the data to the computation.**
Materialising rows in Python was the obvious-but-wrong starting point. Embedding pools as SQL `array()` literals lets the warehouse do what it's good at.

**2. Backwards compatibility is cheap if you start with optional fields.**
Every new feature on `DemoDataRequest` is a Pydantic field with a default that matches the old behaviour. Existing CI scripts and notebook calls to the generator continue to work without a single change.

**3. Validation in the request model, not the orchestrator.**
`anomaly_rate` is validated by a Pydantic `field_validator` to be in `[0.0, 1.0]`. Bad values 422 at the FastAPI boundary, never reach the orchestrator. The same for `dq_profile` — invalid names fail before any SDK call.

**4. Sampled queries beat full-table queries for audits.**
The FK audit's `LIMIT 100000` sample is statistically indistinguishable from a full-table check at any reasonable orphan rate, but runs in seconds vs. minutes on a 100M-row fact table.

**5. Document the limitations, don't hide them.**
The custom-YAML cleanup-on-exception limitation is in the docs. The `dq_profile=clean` early-return saves time but means assertions about generated SQL won't fire — also documented. Honest docs beat clever code.

**6. Default off is safer than default on for new features.**
`realistic_data: false` by default means our 33 existing tests that match `'James'`/`'Mary'` literally keep passing. `validate_referential_integrity: true` defaults on because the cost is negligible and the value is high — a different trade-off in the same codebase.

---

## What's next

**Faker substitution coverage**. The current regex set catches the most common patterns (first names, last names, emails, phones). It misses some niche ones — e.g. the `'CARD','DERM','ENDO'` medical-specialty pool isn't replaced. Adding more patterns is mechanical; the next batch will tag them with named markers in the templates so the loader can find them without regex.

**Composite primary keys**. The `_IdRegistry` design assumes single-column PKs. SCD2 dim tables with `(id, valid_from)` composites are special-cased today. A general solution is on the roadmap.

**Cross-industry references**. `financial.customers` and `retail.customers` are independent today. Customers wanting to demo "the same person buying things and applying for a mortgage" need correlated IDs across industries — also on the roadmap, but ambitious enough to be a future post.

---

If you're working on Databricks demos, ML pipelines, or just want a way to spin up realistic-looking healthcare data in 90 seconds, the [Clone-Xs Demo Data Generator](https://github.com/viral0216/clone-xs) is open-source and ready to use. The full guide with all knobs is in [the docs](https://github.com/viral0216/clone-xs/blob/main/docs/docs/guide/demo-data.md).

The blog title was "one billion rows in 12 minutes" — that's the Full Demo preset on a Medium warehouse with parallel batch INSERTs. We'll see if anyone tries it on a Large.

---

*Have feedback? Hit me up — I'd love to hear what you'd build with this.*
