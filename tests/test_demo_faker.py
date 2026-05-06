"""Tests for src/demo_faker.py — locale-aware synthetic data pools.

Verifies:
1. Pool builders return valid SQL `array(…)` literals
2. Same `seed` → same pool across two `get_faker()` calls (reproducibility)
3. SSN pool always uses the IRS test-pool `9XX-XX-XXXX` format
4. `apply_faker_substitutions` rewrites the legacy pools but leaves
   non-matching expressions verbatim (idempotent)
"""

import re
from unittest.mock import patch

import pytest

# Skip the module entirely if faker isn't installed (it's an optional dep
# at runtime — Clone-Xs's demo data generator only requires it when the
# `realistic_data` feature is enabled).
pytest.importorskip("faker")

from src.demo_faker import (  # noqa: E402
    apply_faker_substitutions,
    email_pool_sql,
    first_name_pool_sql,
    get_faker,
    last_name_pool_sql,
    phone_pool_sql,
    reset_pool_cache_for_tests,
    ssn_pool_sql,
)


@pytest.fixture(autouse=True)
def _clean_cache():
    """Each test starts with empty pool caches so seeded determinism is
    actually testable (otherwise the first test's pool leaks into the next)."""
    reset_pool_cache_for_tests()
    yield
    reset_pool_cache_for_tests()


# ---------------------------------------------------------------------------
# Pool shape — every helper must return a parseable SQL array literal
# ---------------------------------------------------------------------------


def _is_sql_array(literal: str, min_size: int = 1) -> bool:
    """Cheap structural check that the string is a SQL `array('a','b',…)`.
    Doesn't validate the inner SQL, just the shape."""
    if not literal.startswith("array(") or not literal.endswith(")"):
        return False
    inner = literal[len("array(") : -1]
    return inner.count("'") >= min_size * 2


class TestPoolShapes:
    def test_first_name_pool_is_sql_array(self):
        sql = first_name_pool_sql(locale="en_US", seed=42, size=20)
        assert _is_sql_array(sql, min_size=10)

    def test_last_name_pool_is_sql_array(self):
        sql = last_name_pool_sql(locale="en_US", seed=42, size=20)
        assert _is_sql_array(sql, min_size=10)

    def test_email_pool_is_sql_array(self):
        sql = email_pool_sql(locale="en_US", seed=42, size=20)
        assert _is_sql_array(sql, min_size=10)
        # Faker emails use `@example.com` / `.org` / `.net` test domains
        assert "@" in sql

    def test_phone_pool_is_sql_array(self):
        sql = phone_pool_sql(locale="en_US", seed=42, size=20)
        assert _is_sql_array(sql, min_size=10)

    def test_ssn_pool_uses_test_format(self):
        """SSNs MUST use the IRS-reserved 9XX-XX-XXXX test pool format —
        never collides with a real SSN. Critical for compliance: synthetic
        data must not look like it could be a real person's identifier."""
        sql = ssn_pool_sql(seed=42, size=50)
        # Each SSN literal in the array must match the test format.
        ssns = re.findall(r"'(\d{3}-\d{2}-\d{4})'", sql)
        assert len(ssns) == 50
        for ssn in ssns:
            assert ssn.startswith("9"), f"SSN {ssn} doesn't use 9XX-XX-XXXX test pool format"


# ---------------------------------------------------------------------------
# Determinism — seed must produce reproducible pools
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_produces_same_first_name_pool(self):
        a = first_name_pool_sql(locale="en_US", seed=42, size=50)
        reset_pool_cache_for_tests()
        b = first_name_pool_sql(locale="en_US", seed=42, size=50)
        assert a == b, "seed=42 produced different pools on two runs — determinism broken"

    def test_different_seeds_produce_different_pools(self):
        a = first_name_pool_sql(locale="en_US", seed=42, size=50)
        reset_pool_cache_for_tests()
        b = first_name_pool_sql(locale="en_US", seed=999, size=50)
        # Cosmically possible they're identical but vanishingly unlikely;
        # either way the test catches the most common bug (seed ignored).
        assert a != b, "different seeds produced identical pools — seed not honoured"

    def test_get_faker_is_cached(self):
        """get_faker should return the SAME instance for the same (locale, seed) —
        required so the seeded random state actually flows through pool builders."""
        f1 = get_faker("en_US", seed=42)
        f2 = get_faker("en_US", seed=42)
        assert f1 is f2


# ---------------------------------------------------------------------------
# Locale — different locales produce different name distributions
# ---------------------------------------------------------------------------


class TestLocale:
    def test_de_DE_first_names_differ_from_en_US(self):
        """Sanity check: German Faker locale should produce a different
        first-name distribution than American English. (If they're
        identical, the locale arg is being ignored.)"""
        en = first_name_pool_sql(locale="en_US", seed=42, size=50)
        reset_pool_cache_for_tests()
        de = first_name_pool_sql(locale="de_DE", seed=42, size=50)
        assert en != de


# ---------------------------------------------------------------------------
# apply_faker_substitutions — the contract that bridges Faker to
# the existing INDUSTRIES insert_expr templates
# ---------------------------------------------------------------------------


class TestApplyFakerSubstitutions:
    def test_replaces_legacy_first_name_pool(self):
        """The legacy 'James','Mary','John'… first-name array must be replaced
        with a Faker-driven array. The replacement is still a sample
        expression (element_at(...))."""
        legacy = (
            "id + {offset} AS patient_id, "
            "element_at(array('James','Mary','John','Patricia','Robert',"
            "'Jennifer','Michael','Linda','David','Elizabeth'),"
            "cast(floor(rand()*10)+1 as INT)) AS first_name"
        )
        out = apply_faker_substitutions(legacy, locale="en_US", seed=42)
        # Legacy literal should be gone
        assert "'James','Mary','John','Patricia'" not in out
        # Replacement must still be a sampling element_at() expression
        assert "element_at(" in out
        assert "first_name" in out  # column alias preserved

    def test_replaces_legacy_email_pattern(self):
        """`concat('patient',id,'@example.com')` should become a Faker-pool
        sample. The column alias must survive."""
        legacy = "concat('patient',id,'@example.com') AS email"
        out = apply_faker_substitutions(legacy, locale="en_US", seed=42)
        assert "concat('patient',id,'@example.com')" not in out
        assert "AS email" in out
        assert "element_at(" in out

    def test_replaces_legacy_phone_pattern(self):
        """The 555- phone pattern should become a locale-correct Faker phone."""
        legacy = "concat('555-',lpad(cast(floor(rand()*9999999) as STRING),7,'0')) AS phone"
        out = apply_faker_substitutions(legacy, locale="en_US", seed=42)
        assert "concat('555-'" not in out
        assert "AS phone" in out
        assert "element_at(" in out

    def test_idempotent(self):
        """Running substitution twice must equal running once — important
        because some industries' insert_expr go through multiple rewrite
        steps and we need the realism pass to be safely re-applied."""
        legacy = (
            "id, "
            "element_at(array('James','Mary','John','Patricia','Robert',"
            "'Jennifer','Michael','Linda','David','Elizabeth'),"
            "cast(floor(rand()*10)+1 as INT)) AS first_name, "
            "concat('patient',id,'@example.com') AS email"
        )
        once = apply_faker_substitutions(legacy, locale="en_US", seed=42)
        twice = apply_faker_substitutions(once, locale="en_US", seed=42)
        assert once == twice

    def test_leaves_non_matching_expressions_alone(self):
        """An insert_expr that doesn't contain any of the legacy patterns
        must come through verbatim. The substitution is opt-in per-pattern
        — no unintended rewrites of unrelated SQL."""
        plain = (
            "id + {offset} AS order_id, "
            "round(rand()*1000,2) AS amount, "
            "current_timestamp() AS created_at"
        )
        assert apply_faker_substitutions(plain, locale="en_US", seed=42) == plain

    def test_missing_faker_dep_raises_clear_error(self):
        """If faker isn't installed, the import-time error message must
        point users at the fix, not just `ModuleNotFoundError`."""
        # Reset cache so get_faker can be called fresh inside the patched env
        reset_pool_cache_for_tests()
        with patch.dict("sys.modules", {"faker": None}):
            with pytest.raises(ImportError, match="pip install faker"):
                get_faker("en_US")
