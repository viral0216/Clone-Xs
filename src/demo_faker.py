"""Locale-aware synthetic data pools for the Demo Data Generator.

The Demo Data Generator's row generation runs entirely on the SQL warehouse
via ``INSERT INTO target SELECT … FROM (SELECT explode(sequence(1, N)) AS id)``
patterns. Per-column expressions like ``element_at(array('James','Mary',…),…)``
sample from small static pools embedded in the SQL.

This module upgrades those pools to be locale-aware and Faker-driven without
restructuring the generation pipeline:

1. ``get_faker(locale, seed)`` returns a Faker instance with seeded random
   state (so ``seed=42`` produces reproducible output across runs).
2. The ``*_pool_sql`` helpers build the SQL ``array('Alice','Bob',…)`` literal
   from a Faker-generated batch of values. The literal is dropped into the
   ``insert_expr`` template at runtime by ``apply_faker_substitutions``.
3. ``apply_faker_substitutions(insert_expr, locale, seed)`` performs targeted
   regex replacements on the existing INDUSTRIES insert_expr templates —
   common first-name pools, surname pools, the ``concat('patient',id,
   '@example.com')`` email pattern, and the ``concat('555-',lpad(…))`` phone
   pattern. Substitution is opt-in: callers pass ``realistic_data=True`` to
   ``generate_demo_catalog`` to enable. When False, the generator behaves
   exactly as it did before this module existed.

Why we don't materialise rows in Python: the generator produces 1B+ rows at
``scale_factor=1.0``. Pulling those through a Python loop just to call
``faker.first_name()`` per row would be glacial and would defeat the
warehouse-side parallelism. Pre-computing pools (1,000 - 10,000 entries)
and embedding them as SQL array literals gets ~99% of Faker's variability
at near-zero overhead.
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache

logger = logging.getLogger(__name__)


# Default pool size: large enough that any given table sees significant
# variety (10,000 unique names → ~10,000 unique rows before collisions),
# small enough that the SQL literal stays well under Spark's 64 MB query
# size limit even when the array is concatenated into 200+ insert_expr
# templates per generation.
_DEFAULT_POOL_SIZE = 1000


@lru_cache(maxsize=64)
def get_faker(locale: str = "en_US", seed: int | None = None):
    """Return a Faker instance seeded for reproducibility.

    Cached per ``(locale, seed)`` pair so repeated callers within one
    generation share state — important for seeded determinism across the
    pool builders below.
    """
    try:
        from faker import Faker  # type: ignore
    except ImportError as e:  # pragma: no cover — covered by env setup
        raise ImportError(
            "Faker not installed. The Demo Data Generator's `realistic_data` "
            "feature requires `pip install faker>=20.0`."
        ) from e

    f = Faker(locale)
    if seed is not None:
        Faker.seed(seed)
        f.seed_instance(seed)
    return f


def _sql_quote(value: str) -> str:
    """Single-quote escape for embedding inside a SQL string literal."""
    return value.replace("'", "''")


def _array_sql(values: list[str]) -> str:
    """Build the SQL ``array('a','b',…)`` literal from a Python list."""
    quoted = ",".join(f"'{_sql_quote(v)}'" for v in values)
    return f"array({quoted})"


# ---------------------------------------------------------------------------
# Pool builders — return the SQL literal `array('a','b',...)` directly.
# Use the module-level cache so the same locale/seed/size only generates
# the pool once per process.
# ---------------------------------------------------------------------------


@lru_cache(maxsize=128)
def first_name_pool_sql(locale: str = "en_US", seed: int | None = None,
                        size: int = _DEFAULT_POOL_SIZE) -> str:
    """SQL `array('Alice','Bob',…)` literal of `size` first names in locale."""
    f = get_faker(locale, seed)
    names = list({f.first_name() for _ in range(size * 2)})[:size]
    return _array_sql(names or ["Alice"])


@lru_cache(maxsize=128)
def last_name_pool_sql(locale: str = "en_US", seed: int | None = None,
                       size: int = _DEFAULT_POOL_SIZE) -> str:
    f = get_faker(locale, seed)
    names = list({f.last_name() for _ in range(size * 2)})[:size]
    return _array_sql(names or ["Smith"])


@lru_cache(maxsize=128)
def city_pool_sql(locale: str = "en_US", seed: int | None = None,
                  size: int = 200) -> str:
    f = get_faker(locale, seed)
    cities = list({f.city() for _ in range(size * 2)})[:size]
    return _array_sql(cities or ["Springfield"])


@lru_cache(maxsize=128)
def email_pool_sql(locale: str = "en_US", seed: int | None = None,
                   size: int = _DEFAULT_POOL_SIZE) -> str:
    """Email pool — Faker uses RFC-5322-valid synthetic emails by default
    (e.g. `bob.smith@example.org`). Beats the legacy
    `patient1@example.com` pattern for screenshot demos."""
    f = get_faker(locale, seed)
    emails = list({f.email() for _ in range(size * 2)})[:size]
    return _array_sql(emails or ["user@example.com"])


@lru_cache(maxsize=128)
def phone_pool_sql(locale: str = "en_US", seed: int | None = None,
                   size: int = 500) -> str:
    """Phone pool — Faker produces locale-correct format (e.g. en_US uses
    NANP `(555) 123-4567`, en_GB uses `+44 20 7946 0958`)."""
    f = get_faker(locale, seed)
    phones = list({f.phone_number() for _ in range(size * 2)})[:size]
    return _array_sql(phones or ["555-555-5555"])


@lru_cache(maxsize=128)
def ssn_pool_sql(seed: int | None = None, size: int = _DEFAULT_POOL_SIZE) -> str:
    """SSN pool — uses the IRS-reserved `9XX-XX-XXXX` test pool format
    that's guaranteed never to collide with a real-world SSN. Independent
    of locale (SSN is US-specific; non-US locales should use a different
    national-id field)."""
    import random
    rng = random.Random(seed)
    ssns = []
    seen = set()
    while len(ssns) < size:
        s = f"9{rng.randint(0, 99):02d}-{rng.randint(0, 99):02d}-{rng.randint(0, 9999):04d}"
        if s not in seen:
            seen.add(s)
            ssns.append(s)
    return _array_sql(ssns)


@lru_cache(maxsize=128)
def street_address_pool_sql(locale: str = "en_US", seed: int | None = None,
                            size: int = 500) -> str:
    f = get_faker(locale, seed)
    addrs = list({f.street_address().replace("\n", " ") for _ in range(size * 2)})[:size]
    return _array_sql(addrs or ["1 Main St"])


# ---------------------------------------------------------------------------
# Substitution — find legacy patterns in insert_expr and swap in pool SQL.
# ---------------------------------------------------------------------------


def _sample_expr(pool_sql: str) -> str:
    """Build the `element_at(array(...),cast(floor(rand()*N)+1 as INT))`
    expression that samples one row from the pool. ``N`` is computed from
    the comma count in the array literal."""
    # The array literal always opens with `array(`; count commas inside
    # to derive the size. Cheaper than re-parsing.
    inner = pool_sql[len("array("):-1]
    # `inner` is `'a','b','c'` — element count is comma_count + 1
    elements = inner.count(",") + 1 if inner else 1
    return (
        f"element_at({pool_sql},cast(floor(rand()*{elements})+1 as INT))"
    )


# Patterns that legacy INDUSTRIES insert_expr templates use. Each tuple is
# (regex, replacement_sql_builder). Builders accept (locale, seed) so the
# resulting pool is locale-aware.
#
# These regexes deliberately match the EXISTING small pools (10-element
# arrays of common American first/last names, the `patient<id>@example.com`
# email pattern, the `555-XXXXXXX` phone pattern). Anything that doesn't
# match is left alone — additions to INDUSTRIES that happen to use a
# different pattern still work, they just won't get the realism upgrade.

# First names — historic pool starts with 'James','Mary','John','Patricia'…
_FIRST_NAME_RE = re.compile(
    r"element_at\(array\(\s*'James'\s*,\s*'Mary'[^)]+\)\s*,\s*cast\(floor\(rand\(\)\*\d+\)\+1 as INT\)\)"
)
# Provider/employee first-name pool — starts with 'Sarah','David','Emily'…
_FIRST_NAME_RE_2 = re.compile(
    r"element_at\(array\(\s*'Sarah'\s*,\s*'David'[^)]+\)\s*,\s*cast\(floor\(rand\(\)\*\d+\)\+1 as INT\)\)"
)
# Surnames — historic pool starts with 'Smith','Johnson','Williams'…
_LAST_NAME_RE = re.compile(
    r"element_at\(array\(\s*'Smith'\s*,\s*'Johnson'[^)]+\)\s*,\s*cast\(floor\(rand\(\)\*\d+\)\+1 as INT\)\)"
)
# Surname pool variant — provider table uses 'Chen','Patel','Kim'…
_LAST_NAME_RE_2 = re.compile(
    r"element_at\(array\(\s*'Chen'\s*,\s*'Patel'[^)]+\)\s*,\s*cast\(floor\(rand\(\)\*\d+\)\+1 as INT\)\)"
)
# Email pattern: concat('<word>',id,'@example.com')
_EMAIL_RE = re.compile(
    r"concat\('[a-z_]+',\s*id\s*,\s*'@example\.com'\)"
)
# Phone pattern: concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0'))
_PHONE_RE = re.compile(
    r"concat\('555-',\s*lpad\(cast\(floor\(rand\(\)\*9999999\) as STRING\),\s*7\s*,\s*'0'\)\)"
)


def apply_faker_substitutions(
    insert_expr: str, locale: str = "en_US", seed: int | None = None,
) -> str:
    """Rewrite a single INSERT expression to use Faker-driven pools.

    Returns the rewritten string (or the original verbatim if no pattern
    matched). Idempotent — running twice produces the same output as once.
    """
    fn_sample = _sample_expr(first_name_pool_sql(locale, seed))
    ln_sample = _sample_expr(last_name_pool_sql(locale, seed))
    em_sample = _sample_expr(email_pool_sql(locale, seed))
    ph_sample = _sample_expr(phone_pool_sql(locale, seed))

    out = insert_expr
    out = _FIRST_NAME_RE.sub(fn_sample, out)
    out = _FIRST_NAME_RE_2.sub(fn_sample, out)
    out = _LAST_NAME_RE.sub(ln_sample, out)
    out = _LAST_NAME_RE_2.sub(ln_sample, out)
    out = _EMAIL_RE.sub(em_sample, out)
    out = _PHONE_RE.sub(ph_sample, out)
    return out


def reset_pool_cache_for_tests() -> None:
    """Clear the lru_cache decorators. Test-only — not part of the public API."""
    first_name_pool_sql.cache_clear()
    last_name_pool_sql.cache_clear()
    city_pool_sql.cache_clear()
    email_pool_sql.cache_clear()
    phone_pool_sql.cache_clear()
    ssn_pool_sql.cache_clear()
    street_address_pool_sql.cache_clear()
    get_faker.cache_clear()
