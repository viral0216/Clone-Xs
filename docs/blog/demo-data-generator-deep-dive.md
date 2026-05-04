# Synthetic Data on Databricks, the Whole Story — Billions of Rows for Batch, Millisecond Streams for Live Demos

How we built Clone-Xs's Demo Data Generator: a SQL-first batch engine for ~180 tables across 10 industries, a streaming sibling that emits IoT events to Volumes / Bronze / Zerobus, locale-aware Faker pools, ML-ready labeled targets, validated foreign keys — all without leaving the warehouse.

> Disclaimer: I'm writing about the Demo Data Generator that ships inside Clone-Xs (the Databricks Unity Catalog cloning toolkit). Both the batch and streaming sides are implemented and tested; the code samples are simplified for readability. See [`src/demo_generator.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_generator.py), [`src/demo_faker.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_faker.py), and [`src/demo_streaming.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_streaming.py) for the real thing.

---

## The setup: why a "demo data generator" is a real engineering problem

Picture three teams that need synthetic data, all on the same Tuesday:

1. **Sales engineering** has a 14:00 demo with a healthcare insurer. The slides reference a `claims` table, an `encounters` table, joins between `patients` and `providers`, and a churn-risk dashboard. The presenter wants the audience to see *real-looking* names — not `patient1@example.com`.
2. **A solutions architect** is building a fraud-detection notebook. They need 100M `transactions` with a configurable 2% positive class on `is_fraud`, plus enough `customers` and `merchants` rows that the joins return data.
3. **CI** kicks off at 14:00 too. The build needs to validate that 200 table DDL templates parse correctly across the warehouse's runtime — but it can't afford a 90-minute generation step.

Then a fourth team comes in the door:

4. **An IoT customer evaluation** wants to see live data flowing into a Bronze table — gauges ticking, an Auto Loader pipeline catching new files, the Lakeflow status panel updating in real time. Static rows in a Delta table won't sell it. They need an *emitter*, not a snapshot.

These four workloads don't share a single tool. Faker on its own can't get to a billion rows in any reasonable time. Mockaroo doesn't know about your Delta Sharing topology. Snowflake's data generators don't run on Databricks. The hand-rolled "loop with `time.sleep(5)`" notebook works for one demo and falls apart for the next. And the legacy in-house solution — a hand-rolled notebook with hard-coded `'James','Mary'` arrays — doesn't satisfy any of the four.

What we wanted was one toolkit with two siblings:

- A **batch** generator that produces a 10-industry, ~180-table catalog at any scale from 10M to 10B rows, with realistic names / SSNs / phones, ML-ready labeled targets, validated foreign keys, schema-only mode for CI, and YAML extension hooks.
- A **streaming** sibling that emits continuous event batches across 10 device profiles, picks one of four destinations (Volume / Volume+Bronze / direct INSERT / Zerobus low-latency gRPC), and runs as either an in-process loop or a scheduled Databricks Job.

This post walks through how the [Clone-Xs Demo Data Generator](https://github.com/viral0216/clone-xs) does both, why each design decision was made, and what we learned along the way.

---

## When to pick which

Before the deep dive — a quick decision table, because the most common question we hear is "do I want batch or streaming?":

| You need… | Pick | Why |
|---|---|---|
| A static catalog with millions of rows for joins / dashboards / training | **Batch** | One SQL `INSERT` per table, vectorised across the warehouse |
| A schema-only catalog for CI to lint DDL against | **Batch** (`schema_only: true`) | ~30 seconds for 200 tables; no row materialisation |
| A Kimball star schema (fact + dim + `dim_date`) for BI tools | **Batch** (`data_model: star_schema`) | CTAS overlay on top of the flat layer; +5% time |
| Continuous events landing in a Bronze table you can `STREAM read_files()` over | **Streaming** (Volume + Bronze) | File-based; uses Auto Loader / DBSQL streaming tables |
| Lowest-latency event → Delta path (sub-second durability, seconds-to-table) | **Streaming** (Zerobus) | Direct gRPC append into Delta — no Volume hop |
| An IoT demo that must run unattended for 24h after you log off | **Streaming** (scheduled Job) | Generates a notebook + Databricks Job with a Quartz cron |

If you're not sure which you want: pick **Batch** to seed the dimensions (customers, devices, merchants), then layer **Streaming** for the fact-table firehose on top. That's the production shape and it works just as well as a demo shape.

The two siblings share a lot of design vocabulary — same Pydantic request models, same dispatch-via-registry pattern, same opinionated "fail open, surface in the result" posture — so the rest of this post can cover both without doubling its length.

---

# Part 1 — Batch: SQL all the way down

## Architecture: never materialise rows in Python

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

The generator emits SQL like this; Spark expands `sequence(1, 5_000_000)` into 5M rows in parallel across the warehouse's cores; `rand()` and `element_at()` are vectorised. A 5M-row batch lands in seconds, not minutes. Sustained throughput on a Medium warehouse hits roughly 1.5M rows/sec — so a billion rows is a sequence of 200 such batches that finishes in roughly 11 minutes of warehouse time, plus per-table fixed overhead. The point isn't "billions in seconds" — it's that the per-row cost is a SQL expression on the warehouse, not a Python tuple over the JDBC wire.

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

This decision — **express data shape as SQL string templates, not Python row generators** — is what makes the rest of Part 1 possible.

---

## Realism — Faker without leaving the warehouse

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

## DQ profiles + ML labels

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

## Referential integrity (or, "why doesn't my JOIN return rows?")

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

## Schema-only mode (the CI win)

CI doesn't care about row counts. CI cares whether the 200-table DDL templates parse correctly on the warehouse's current runtime version.

The schema-only flag walks the orchestrator's normal path — CREATE CATALOG, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION, CREATE VOLUME — but skips every INSERT. Volumes still create as DDL but skip the sample CSV writes. DQ injection, version history, seasonal patterns, and labeled anomaly columns all early-return.

Generation drops from "tens of minutes at scale 1.0" to **tens of seconds**. The CI step is now: spin up a serverless warehouse, run `generate_demo_catalog(schema_only=true)`, assert it returns clean, drop the catalog. A regression in any DDL template fails the build before code review.

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

50 lines of code; immeasurable amount of CI-cycles saved.

---

## Live preview — pure arithmetic, zero Databricks calls

Users picking 10 industries at scale 1.0 deserve to know that's about to take tens of minutes and produce a few hundred GB before they click Generate. The preview endpoint computes this without going near the warehouse:

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

## Custom YAML industries — extension without forking

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

## Star-schema overlay — Kimball without re-generating data

The flat industry tables are useful for ad-hoc demos, but BI teams want fact tables and dim tables. Tableau / Power BI / dbt-style modelling is the actual job-to-be-done for a lot of customer evaluations. So we layered a Kimball-style star schema on top of the flat layer.

The trick: don't regenerate. The star schema is a **CTAS overlay** — `CREATE TABLE … AS SELECT` from the existing flat tables, which adds ~5% to total generation time vs. doubling it.

Triggered by `data_model="star_schema"`, the orchestrator builds a parallel `<industry>_star` schema next to the flat one. Naming follows DBT-style + DV2 conventions:

- Schema: `healthcare_star`
- Fact: `fct_claims`, `fct_encounters`, …
- Dim: `dim_patient`, `dim_provider`, `dim_facility`, …
- Surrogate key: `patient_sk` (BIGINT, `row_number()` over the business key)
- Business key preserved as `patient_id` etc.

The per-industry split lives in a registry, not in the orchestrator code:

```python
# src/demo_models.py
STAR_SCHEMA_REGISTRY = {
    "healthcare": {
        "dims": [
            ("dim_patient",  "patients",   "patient_id"),
            ("dim_provider", "providers",  "provider_id"),
            ("dim_facility", "facilities", "facility_id"),
        ],
        "facts": [
            ("fct_claims", "claims", [
                ("patient_id",  "dim_patient"),
                ("provider_id", "dim_provider"),
                ("facility_id", "dim_facility"),
            ]),
            # …
        ],
        "derived_dims": [
            ("dim_diagnosis", "claims", "diagnosis_code"),
        ],
    },
    # …
}
```

Adding an industry is a dict entry — no code change.

**Three dim types, one CTAS pattern:**

*Conformed dims* — `row_number()` surrogate key over the existing flat dim:

```sql
CREATE OR REPLACE TABLE healthcare_star.dim_patient AS
SELECT row_number() OVER (ORDER BY patient_id) AS patient_sk, *
FROM healthcare.patients
```

*Derived dims* — for attributes that live as a column on a fact (e.g. `claims.diagnosis_code`) but deserve their own dim:

```sql
CREATE OR REPLACE TABLE healthcare_star.dim_diagnosis AS
SELECT row_number() OVER (ORDER BY diagnosis_code) AS diagnosis_sk, diagnosis_code
FROM (SELECT DISTINCT diagnosis_code FROM healthcare.claims WHERE diagnosis_code IS NOT NULL)
```

*Universal `dim_date`* — built from the same `explode(sequence(…))` primitive that powers the entire row generator. The architecture trick from the opening reappears here, on a smaller scale:

```sql
SELECT cast(d AS DATE) AS date_key, year(d), quarter(d), month(d), …
FROM (SELECT explode(sequence(date('2020-01-01'), date('2025-01-01'), interval 1 day)) AS d)
```

**Facts** pass through with `LEFT JOIN`s to each dim, pulling the surrogate keys onto the row:

```sql
CREATE OR REPLACE TABLE healthcare_star.fct_claims AS
SELECT f.*, d0.patient_sk, d1.provider_sk, d2.facility_sk
FROM healthcare.claims f
LEFT JOIN healthcare_star.dim_patient   d0 ON f.patient_id  = d0.patient_id
LEFT JOIN healthcare_star.dim_provider  d1 ON f.provider_id = d1.provider_id
LEFT JOIN healthcare_star.dim_facility  d2 ON f.facility_id = d2.facility_id
```

`schema_only=true` produces zero-row CTASs of the same shape via `WHERE 1=0` — useful for CI and permission-scoping rehearsals.

Per-industry failures don't abort the loop — partial star coverage beats none, and the per-industry report bubbles up to the run result so the UI can render the gap.

What's not in v1:
- SCD2 history is *carried through* (the flat dim source already has `valid_from` / `valid_to` / `is_current`), but no row-level history is built in the star — single row per business key.
- Data Vault 2.0, One Big Table, and Snowflake variants are deferred. The registry shape was designed to admit them: `data_model` is an enum, not a boolean.

---

# Part 2 — Streaming: continuous events for live demos

The batch generator answers "give me a static catalog of N rows." It can't answer "give me a Bronze table that's *being written to right now*, so the audience can watch the row count tick up while I narrate." That's a different shape — and it deserved its own module rather than a `streaming: true` flag bolted onto the batch path.

So we built the streaming sibling: [`src/demo_streaming.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_streaming.py). Same opinionated defaults, same Pydantic boundary, but a fundamentally different runtime — a background loop that emits batches on a tunable cadence, with four destination strategies the user picks per run.

## What "streaming" means here

The streaming sibling is **simulated** event emission, not a real Kafka cluster. It's designed for demos where the audience needs to *see* a stream — gauges ticking, file counts climbing, a Lakeflow job consuming new data. It is not designed to load-test production Kafka topics.

What it does:

- Every N seconds, generate a batch of M events for one of 10 device profiles.
- Land that batch wherever the operator picked (Volume / Volume + Bronze / direct INSERT / Zerobus).
- Update a progress dict on every tick so the existing `/jobs` polling endpoint surfaces live counts to the UI.
- Run either as an in-process background thread inside the API server (good for ad-hoc demos that end with the browser tab) or as a scheduled Databricks Job with a Quartz cron (good for "leave it running for 24 hours").

What it doesn't do:

- It doesn't claim per-event latency below the cadence interval. A 5-second cadence means 5-second batches.
- It doesn't replay historical data. Each tick generates fresh events with the current `datetime.now(timezone.utc)`.
- It doesn't manage backpressure between the emitter and the downstream consumer. If your Bronze table can't keep up, files queue up in the Volume — same as any Auto Loader flow.

---

## Device profiles — 10 generators, one signature

Every profile is two functions: an `init_state(num_devices)` that builds the per-device stateful baseline (mean RPM, baseline SpO2, tool wear so far, etc.), and a `generate_event(state, seq, now)` that emits one row dict.

The 10 built-ins cover the common asks across the supported industries:

| Profile | Industry | What ticks |
|---|---|---|
| `generic_sensor` | (any) | temperature, humidity, pressure, vibration |
| `industrial_machine` | manufacturing | RPM, oil pressure, tool wear (monotonic), occasional DTCs |
| `car_obd2` | automotive | speed, RPM, fuel level, lat/lng |
| `smart_meter` | energy | cumulative kWh, voltage, current, power factor |
| `wearable_health` | healthcare | heart rate, SpO2, steps, alerts |
| `pos_terminal` | retail | sale amount, payment method, status |
| `wind_turbine` | energy | wind speed, RPM, power output, blade pitch, faults |
| `atm_transaction` | financial | withdrawal/deposit, lat/lng, fraud flag |
| `server_metrics` | infra | CPU / memory / disk / network per host |
| `clickstream` | digital | session, event, page, user-agent |

The `industrial_machine` profile is the one I usually point people at to understand the design — it's small enough to read in one screen and shows every interesting decision:

```python
def _gen_industrial_machine(state: dict, seq: int, now: datetime) -> dict:
    """Industrial machine telemetry: RPM, oil pressure, tool wear, DTCs.

    Tool-wear monotonically increases per machine across batches —
    realistic for cumulative wear demos. ~3% of events carry an error
    code (DTC like `E12`) for anomaly demos.
    """
    devices: list[dict] = state["devices"]
    d = devices[seq % len(devices)]
    d["tool_wear_pct"] = min(100.0, d["tool_wear_pct"] + random.uniform(0.001, 0.01))
    error_code = None
    if random.random() < 0.03:
        error_code = f"E{random.randint(10, 99)}"
    return {
        "machine_id": d["id"],
        "captured_at": now.isoformat(),
        "rpm": int(d["rpm_mean"] + random.uniform(-50.0, 50.0)),
        "oil_pressure_psi": round(d["oil_pressure_mean"] + random.uniform(-2.0, 2.0), 2),
        "tool_wear_pct": round(d["tool_wear_pct"], 4),
        "error_code": error_code,
    }
```

Two things worth pointing out:

1. **The state is mutable per device, mutated in place.** Tool wear advances a few thousandths of a percent per event and never resets. Over a 24-hour stream that's a clean monotonically-increasing signal a maintenance demo can train a model against.
2. **Most fields jitter around a per-device baseline rather than spanning the full possible range.** RPM moves ±50 around a mean, not 0..10000 uniform. This is what real telemetry looks like and is the difference between "demo data" and "demo data that fools the audience."

Adding a new profile is one entry in `DEVICE_PROFILES` (registry pattern, mirrors the batch side's `INDUSTRIES`):

```python
DEVICE_PROFILES = {
    "industrial_machine": (_init_state_industrial_machine, _gen_industrial_machine),
    # ... others
}
```

---

## Architecture: four destinations, one dispatcher

The Pydantic request model has a single `destination` field that switches between four strategies. The orchestrator branches on it once at start, then the per-tick code path is identical:

```python
destination = config["destination"]  # "volume" | "volume_bronze" | "direct_table" | "zerobus"

if destination not in ("volume", "volume_bronze", "direct_table", "zerobus"):
    raise ValueError(f"Unknown destination: {destination!r}")

# Open the destination once — the per-tick loop below picks the same
# branch every iteration. For Zerobus this is the gRPC stream open;
# for Volume / Bronze it's just a path string; for direct_table it's
# the pre-flight CREATE TABLE.
sink = open_sink(destination, config)

try:
    while not stopped() and not deadline_passed():
        now = datetime.now(timezone.utc)
        records = emit_batch(profile, state, events_per_batch, base_seq=seq)
        write_to_sink(sink, records, now, seq)
        progress["events_emitted"] += len(records)
        seq += len(records)
        time.sleep(interval_seconds)
finally:
    close_sink(sink)
```

The four destinations differ in what `open_sink` / `write_to_sink` / `close_sink` do, not in the loop shape. That uniformity is what kept the streaming runtime modest — `demo_streaming.py` is ~1.4k LOC and the Zerobus runtime adds ~500 LOC on top — even with the four destinations piled in.

### Destination 1: `volume` — JSON files in a UC Volume

The simplest strategy: each tick writes one JSON file (one record per line, NDJSON-style) to a Volume sub-path keyed on the profile and a UTC ISO timestamp. The user wires Auto Loader / DLT downstream however they want.

```python
def write_batch_to_volume(client, catalog, schema, volume, profile, records, now, seq):
    file_name = f"batch-{now.strftime('%Y%m%dT%H%M%SZ')}-{seq:08d}.json"
    file_path = f"/Volumes/{catalog}/{schema}/{volume}/{profile}/{file_name}"
    body = "\n".join(json.dumps(r, separators=(',', ':')) for r in records)
    client.files.upload(file_path=file_path, contents=io.BytesIO(body.encode("utf-8")))
```

Output paths:
```
/Volumes/<catalog>/<schema>/<volume>/<profile>/batch-<isoZ>-<seq>.json
```

Use this when the demo is "show how Auto Loader picks up new files" or when the customer's pipeline already exists and just needs a source.

### Destination 2: `volume_bronze` — Volume + auto-Bronze STREAMING TABLE

Same Volume emission as above, plus a one-time `CREATE OR REFRESH STREAMING TABLE` over the Volume path so a Bronze Delta table fills in as files land:

```python
def create_bronze_streaming_table(client, warehouse_id, catalog, schema, profile, refresh_minutes=5):
    table_fqn  = f"`{catalog}`.`{schema}`.`bronze_{profile}`"
    volume_path = f"/Volumes/{catalog}/{schema}/events_volume/{profile}/"
    cron_expr  = f"0 0/{refresh_minutes} * * * ?"  # Quartz CRON, portable across DBSQL editions
    sql = (
        f"CREATE OR REFRESH STREAMING TABLE {table_fqn} "
        f"SCHEDULE REFRESH CRON '{cron_expr}' AT TIME ZONE 'UTC' "
        f"AS SELECT * FROM STREAM read_files('{volume_path}', format => 'json')"
    )
    execute_sql(client, warehouse_id, sql)
```

Two design decisions here that took a couple iterations to land on:

- **Quartz CRON, not the `EVERY N MINUTES` shorthand.** The shorthand only works on a subset of DBSQL runtime versions / tiers; the 6-field Quartz syntax is portable across Free Edition, Premium, and Enterprise. Easy to forget until a Free Edition demo blows up.
- **Soft failure, not hard failure.** If `CREATE OR REFRESH STREAMING TABLE` raises (most commonly: warehouse isn't DBSQL Serverless), the orchestrator captures the error and keeps emitting files. The user gets a Bronze "soft fail" line in the result panel and can run the SQL manually after upgrading; the demo doesn't die at 14:01.

This is the destination most demos pick — the operator sees JSON files appearing in the Volume *and* row counts growing in the Bronze table.

### Destination 3: `direct_table` — INSERT INTO Bronze, no Volume

Some demos don't want a Volume in the picture at all. For those, the runtime CREATEs the catalog + schema + Bronze Delta table up-front, then each tick emits a small `INSERT INTO bronze_<profile> VALUES (...), (...), ...` statement directly:

```python
def insert_batch_direct(client, warehouse_id, fqn, records):
    if not records:
        return
    cols = list(records[0].keys())
    rows_sql = ",".join(
        "(" + ",".join(_format_sql_value(r.get(c)) for c in cols) + ")"
        for r in records
    )
    sql = f"INSERT INTO {fqn} ({','.join(cols)}) VALUES {rows_sql}"
    execute_sql(client, warehouse_id, sql)
```

Trade-offs versus `volume_bronze`:

- ✓ No Volume to set up. One-step pipeline.
- ✓ Faster end-to-end visibility — the row appears in `SELECT count(*)` as soon as the INSERT commits, not after the next refresh tick.
- ✗ Each tick is a synchronous SQL round-trip to the warehouse. Higher per-batch latency than file writes.
- ✗ Bigger batches stress the SQL parser (`INSERT INTO … VALUES (…), (…), …, (…)` with 100 row-tuples is a lot of tokens). The default of 100 events / 5 seconds is fine; 10,000 / 1 second is asking for trouble.

### Destination 4: `zerobus` — direct gRPC into Delta, sub-second durability

The newest destination, and the one that justified the Pydantic-managed-strategy refactor. **Zerobus** is Databricks' low-latency ingest API: a gRPC stream that appends directly into a Delta table without a Volume, Auto Loader, or DLT in the loop. Published SLAs are P95 durability ≤ 500ms (the bytes are committed) and P95 time-to-table ≤ 30s (the row shows up in `SELECT`) — so for "did the event land safely" the answer comes back sub-second; for "is it queryable" it's seconds, not the minutes a 5-minute Bronze refresh would impose.

The runtime path is opt-in because it adds three constraints:

1. **The Zerobus Python SDK** (`databricks-zerobus-ingest-sdk`) must be installed in the API server's Python environment.
2. **A service principal** with USE_CATALOG, USE_SCHEMA, MODIFY+SELECT grants on the destination table.
3. **A region-specific gRPC endpoint** of the form `https://<workspace_id>.zerobus.<region>.cloud.databricks.com`, which Clone-Xs derives from the workspace URL via a DNS CNAME walk (so the operator never has to look it up).

The dispatch is the same shape as the others — `open_zerobus_stream`, `ingest_batch_zerobus`, `close_zerobus_stream` — but the lifecycle matters more here. Opening a fresh gRPC stream per batch defeats the entire point. The orchestrator opens the stream once before the emission loop, hands the handle to the per-tick ingest function, and closes it in a `finally` so a stream never leaks even when the loop is interrupted:

```python
sdk = ZerobusSdk(server_endpoint, workspace_url)
stream = sdk.create_stream(
    client_id, client_secret,
    TableProperties(table_fqn),
    StreamConfigurationOptions(record_type=RecordType.JSON),
)
try:
    while not stopped() and not deadline_passed():
        records = emit_batch(profile, state, events_per_batch, base_seq=seq)
        last_offset = None
        for r in records:
            last_offset = stream.ingest_record_offset(r)
        # Block on the LAST offset's durability ack — durability is
        # monotonic, so confirming the last offset implicitly confirms
        # every prior offset in the batch.
        if last_offset is not None:
            stream.wait_for_offset(last_offset)
        seq += len(records)
        time.sleep(interval_seconds)
finally:
    stream.close()
```

We also generate a **copy-paste-runnable Python snippet** for users who want to try Zerobus from their own laptop without installing the SDK in the API server. The snippet is rendered by [`src/demo_streaming_zerobus.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_streaming_zerobus.py), which pulls the per-profile generator source out of the same registry the in-process runtime uses — so the snippet's behaviour is identical to what the in-process loop would emit.

A `wait_for_offset` per batch (rather than per record) is the canonical Databricks-docs pattern. Production code that prefers throughput over per-batch confirmation drops it in favour of `AckCallback`, which is documented but out of scope for the demo path.

### One last subtlety: stream lifecycle is the bug everyone writes once

The temptation when wiring a new destination is to open + close per batch — it makes the per-tick code "self-contained." Don't. The Zerobus gRPC handshake is the canonical example — Databricks documents the lifecycle as "open once, ingest many, close at end" because per-batch opens defeat the latency advantage that makes Zerobus worth picking in the first place. The dispatch puts the open before the loop and the close in a `finally`, and every destination follows that contract whether they technically need to or not. That uniformity is what lets future destinations slot in without their own bespoke teardown ceremony.

---

## Background loop or scheduled Job — same generator, two runtimes

Whichever destination you pick, the emission loop has to *run somewhere*. Two options:

**In-process background thread.** The API server starts a Python thread, the loop ticks until the duration elapses or the user clicks Stop. Lives or dies with the API process — fine for a 30-minute demo, useless for a "leave it running overnight." Implementation is one `threading.Thread(target=run_streaming_emission, ...)` plus a stop-flag dict.

**Scheduled Databricks Job.** [`src/demo_streaming_schedule.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_streaming_schedule.py) uploads a self-contained Python notebook (one cell per profile generator, plus a top-level loop) and creates a Job with a Quartz cron (`every 5 minutes`, `top of hour`, `weekdays at 9am`, etc.). The Job is tagged `created_by=clone-xs, kind=streaming-emit, profile=<name>` so it shows up in the existing `/clone-jobs` listing and the operator can pause / edit / delete via the standard Jobs UI. Survives API server restarts; survives the operator going home.

Both runtimes share the per-profile `init_state` / `generate_event` source. The notebook is generated by string-substituting the generator source into a template (one source file per profile, kept in `_PROFILE_GENERATORS_SOURCE` so the in-process and scheduled paths can never drift). The test suite asserts the in-process state initialiser and the scheduled-Job notebook produce byte-identical first-batch records given the same seed — a regression there would silently break demos.

---

## Numbers and what they mean

A `Quick Demo` preset on a serverless **Small SQL Warehouse** (1 industry, scale 0.01, no medallion). For `healthcare` at scale 0.01 that's ~2M rows across 20 tables:

- **Schema-only batch** (`schema_only: true`): seconds — only DDL hits the warehouse
- **Default batch**: roughly a minute or two — most of the time is per-table fixed overhead (CREATE TABLE + INSERT + audit), not row throughput
- **With realism + anomaly labels + FK audit**: same order of magnitude — Faker pool building is one-time at the start (~2 s), then embedded in SQL for the rest of the run

A `Full Demo` preset on a **Medium SQL Warehouse** (10 industries, scale 1.0 — ~1.94B rows across ~179 tables):

- **Schema-only batch**: tens of seconds (DDL only — no row materialisation)
- **Default batch**: tens of minutes — exact wall-time depends heavily on warehouse size and concurrency
- **With realism + anomaly labels + FK audit**: same order of magnitude — realism is essentially free (Faker pools embedded as SQL `array()` literals), labels add one ALTER + UPDATE per labeled column, audit is sampled
- **Plus star schema overlay** (`data_model: star_schema`): adds ~5% to total time — the overlay is CTAS, not regeneration

**Streaming**, on the same Medium warehouse with the default 100 events / 5 seconds cadence:

- `volume`: ~20 events/sec sustained, ~10–20 KB NDJSON file per batch (size depends on profile field count), no warehouse cost between ticks
- `volume_bronze`: same emission cost; Bronze table refreshes every `refresh_minutes` (default 5) on a small DBSQL Serverless slice
- `direct_table`: ~20 events/sec at the default cadence; each tick is a synchronous `INSERT INTO ... VALUES (...), (...)` round-trip, so a hotter warehouse helps and very large `events_per_batch` values stress the SQL parser
- `zerobus`: ~20 events/sec sustained at default cadence; P95 ≤ 500ms durability (event committed) + P95 ≤ 30s time-to-table (row visible in `SELECT`); gRPC connection stays open for the whole run

Throughput is governed by `events_per_batch` and `interval_seconds`, both configurable. A `1000 / 1.0` (1k events per second) load is well within Zerobus and `volume`; `direct_table` will start to chase the warehouse at that rate.

---

## Lessons learned

**1. Push computation to the data, not the data to the computation.**
Materialising rows in Python was the obvious-but-wrong starting point for batch. Embedding pools as SQL `array()` literals lets the warehouse do what it's good at. The streaming sibling does the opposite — it materialises rows in Python because the cadence is the throttle — and that's fine; the right shape is per-feature, not universal.

**2. Backwards compatibility is cheap if you start with optional fields.**
Every new feature on `DemoDataRequest` and `StreamingRequest` is a Pydantic field with a default that matches the old behaviour. Existing CI scripts and notebook calls continue to work without a single change. The streaming `destination` enum's default falls back to the legacy `auto_create_bronze` flag for the same reason.

**3. Validation in the request model, not the orchestrator.**
`anomaly_rate` is validated by a Pydantic `field_validator` to be in `[0.0, 1.0]`. Bad values 422 at the FastAPI boundary, never reach the orchestrator. The streaming side validates `events_per_batch` and `interval_seconds` against per-environment limits the same way.

**4. Sampled queries beat full-table queries for audits.**
The FK audit's `LIMIT 100000` sample is statistically indistinguishable from a full-table check at any reasonable orphan rate, but runs in seconds vs. minutes on a 100M-row fact table.

**5. Open once, ingest many, close in `finally`.**
Zerobus made this lesson loud, but it applies everywhere — anywhere a dispatched destination has setup cost, the loop should pay it once. A consistent open/close pattern across destinations is what kept the streaming runtime modest in size and predictable in behaviour, even with four destinations to support.

**6. Soft-fail the secondary destination, not the primary.**
If the auto-Bronze CREATE STREAMING TABLE fails (DBSQL Serverless not available, missing privilege), keep emitting files. The operator sees a calm "Bronze: soft-fail, run this SQL manually" line in the result panel, not a crashed demo at 14:01. The Zerobus-runtime / Iceberg-UniForm preflights follow the same posture.

**7. Document the limitations, don't hide them.**
The custom-YAML cleanup-on-exception limitation is in the docs. The `dq_profile=clean` early-return saves time but means assertions about generated SQL won't fire — also documented. The `direct_table` streaming destination has higher per-tick latency than `volume` — also documented. Honest docs beat clever code.

**8. Default off is safer than default on for new features.**
`realistic_data: false` by default means our existing tests that match `'James'`/`'Mary'` literally keep passing. `validate_referential_integrity: true` defaults on because the cost is negligible and the value is high — a different trade-off in the same codebase. Streaming's `destination: "volume"` defaults to the safest path (no warehouse load between ticks); operators opt in to `volume_bronze` / `direct_table` / `zerobus`.

---

## What's next

**Faker substitution coverage**. The current regex set catches the most common patterns (first names, last names, emails, phones). It misses some niche ones — e.g. the `'CARD','DERM','ENDO'` medical-specialty pool isn't replaced. Adding more patterns is mechanical; the next batch will tag them with named markers in the templates so the loader can find them without regex.

**Composite primary keys**. The `_IdRegistry` design assumes single-column PKs. SCD2 dim tables with `(id, valid_from)` composites are special-cased today. A general solution is on the roadmap.

**Cross-industry references**. `financial.customers` and `retail.customers` are independent today. Customers wanting to demo "the same person buying things and applying for a mortgage" need correlated IDs across industries — also on the roadmap, but ambitious enough to be a future post.

**Streaming back-pressure**. When the warehouse can't keep up with `direct_table` ingest, files queue silently in the in-process `_IdRegistry`. Surfacing that via the progress dict ("warehouse lag: ~14 ticks behind") would let demo operators see a yellow flag before the audience does.

**More streaming sources for cross-system demos**. Today every profile generates locally. A next step is a `mode: "tap"` that reads from a Databricks-managed Kafka or an existing Bronze and *transforms* rather than synthesises — useful for "show me my real anonymised data flowing into a new pipeline" demos.

---

If you're working on Databricks demos, ML pipelines, or just want a way to spin up realistic-looking synthetic data — a batch catalog in a couple of minutes or a streaming emitter that runs for as long as you need — the [Clone-Xs Demo Data Generator](https://github.com/viral0216/clone-xs) is open-source and ready to use. The full guide with all knobs is in [the docs](https://github.com/viral0216/clone-xs/blob/main/docs/docs/guide/demo-data.md).

---

*Have feedback? Hit me up — I'd love to hear what you'd build with this.*
