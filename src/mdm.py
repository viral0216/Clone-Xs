"""MDM (Master Data Management) — entity resolution, survivorship, and stewardship."""

import logging
import uuid
from datetime import datetime, timezone

from src.client import execute_sql
from src.mdm_store import MDMStore

logger = logging.getLogger(__name__)

# ---- Fuzzy Matching Utilities ----

def _jaro_winkler(s1: str, s2: str) -> float:
    """Jaro-Winkler similarity (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    search_range = max(len1, len2) // 2 - 1
    if search_range < 0:
        search_range = 0
    matched1 = [False] * len1
    matched2 = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        lo = max(0, i - search_range)
        hi = min(i + search_range + 1, len2)
        for j in range(lo, hi):
            if matched2[j] or s1[i] != s2[j]:
                continue
            matched1[i] = matched2[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not matched1[i]:
            continue
        while not matched2[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    # Winkler boost for common prefix (up to 4 chars)
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


def _levenshtein_ratio(s1: str, s2: str) -> float:
    """Normalized Levenshtein similarity (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    matrix = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    for i in range(len1 + 1):
        matrix[i][0] = i
    for j in range(len2 + 1):
        matrix[0][j] = j
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            matrix[i][j] = min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost)
    distance = matrix[len1][len2]
    return 1.0 - distance / max(len1, len2)


def _soundex(s: str) -> str:
    """American Soundex phonetic encoding."""
    if not s:
        return ""
    s = s.upper()
    codes = {"B": "1", "F": "1", "P": "1", "V": "1", "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2", "S": "2", "X": "2", "Z": "2", "D": "3", "T": "3", "L": "4", "M": "5", "N": "5", "R": "6"}
    result = s[0]
    prev = codes.get(s[0], "0")
    for ch in s[1:]:
        code = codes.get(ch, "0")
        if code != "0" and code != prev:
            result += code
        prev = code if code != "0" else prev
    return (result + "000")[:4]


def _normalize(s: str) -> str:
    """Normalize string for comparison — lowercase, strip, remove common suffixes."""
    if not s:
        return ""
    s = s.lower().strip()
    for suffix in [" llc", " ltd", " inc", " corp", " corporation", " co", " company", " plc", " gmbh", " ag", " sa", " sas", " bv"]:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


# ---- Match Functions ----

MATCH_FUNCTIONS = {
    "exact": lambda a, b: 1.0 if a and b and str(a).strip().lower() == str(b).strip().lower() else 0.0,
    "fuzzy_jaro_winkler": lambda a, b: _jaro_winkler(str(a).lower(), str(b).lower()) if a and b else 0.0,
    "fuzzy_levenshtein": lambda a, b: _levenshtein_ratio(str(a).lower(), str(b).lower()) if a and b else 0.0,
    "soundex": lambda a, b: 1.0 if a and b and _soundex(str(a)) == _soundex(str(b)) else 0.0,
    "normalized": lambda a, b: 1.0 if _normalize(str(a)) == _normalize(str(b)) else _levenshtein_ratio(_normalize(str(a)), _normalize(str(b))) if a and b else 0.0,
    "numeric": lambda a, b: 1.0 if a and b and "".join(c for c in str(a) if c.isdigit()) == "".join(c for c in str(b) if c.isdigit()) else 0.0,
}

# ---- Survivorship Strategies ----

def _survive_most_trusted(values: list[tuple[str, float]]) -> str:
    """Pick value from source with highest trust score."""
    return max(values, key=lambda x: x[1])[0] if values else ""

def _survive_most_recent(values: list[tuple[str, str]]) -> str:
    """Pick value from most recently ingested source."""
    return max(values, key=lambda x: x[1])[0] if values else ""

def _survive_most_complete(values: list[tuple[str, dict]]) -> str:
    """Pick value from source with fewest nulls."""
    return min(values, key=lambda x: sum(1 for v in x[1].values() if not v))[0] if values else ""

def _survive_longest(values: list[str]) -> str:
    """Pick the longest non-empty value."""
    return max(values, key=len) if values else ""


class MDMManager:
    """Entity resolution engine with matching, merging, survivorship, and stewardship."""

    def __init__(self, client, warehouse_id: str, config: dict):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config
        catalog = config.get("audit_trail", {}).get("catalog", "clone_audit")
        self.store = MDMStore(client, warehouse_id, state_catalog=catalog, state_schema="mdm")

    def init_tables(self) -> dict:
        self.store.init_tables()
        return {"status": "ok", "message": "MDM tables initialized"}

    # ---- Entity CRUD ----

    def create_entity(self, entity_type: str, display_name: str, attributes: dict, created_by: str = "") -> dict:
        entity_id = str(uuid.uuid4())
        self.store.upsert_entity(entity_id, entity_type, display_name, attributes, source_count=0, confidence_score=1.0, created_by=created_by)
        return {"entity_id": entity_id, "entity_type": entity_type, "display_name": display_name}

    def get_entities(self, entity_type: str = None, status: str = None, limit: int = 100) -> list:
        return self.store.get_entities(entity_type, status, limit)

    def get_entity_detail(self, entity_id: str) -> dict:
        entity = self.store.get_entity(entity_id)
        sources = self.store.get_source_records(entity_id=entity_id) if entity else []
        return {"entity": entity, "source_records": sources}

    def update_entity(self, entity_id: str, display_name: str, attributes: dict) -> dict:
        entity = self.store.get_entity(entity_id)
        if not entity:
            return {"error": "Entity not found"}
        self.store.upsert_entity(entity_id, entity.get("entity_type", ""), display_name, attributes, entity.get("source_count", 0), entity.get("confidence_score", 0), entity.get("status", "active"))
        return {"entity_id": entity_id, "updated": True}

    def delete_entity(self, entity_id: str) -> dict:
        self.store.delete_entity(entity_id)
        return {"entity_id": entity_id, "deleted": True}

    # ---- Source Record Ingestion ----

    def ingest_source_records(self, catalog: str, schema: str, table: str, entity_type: str, key_column: str, trust_score: float = 1.0) -> dict:
        """Ingest records from a Unity Catalog table as source records."""
        fqn = f"{catalog}.{schema}.{table}"
        source_system = f"{catalog}.{schema}"

        # Get column names
        cols = execute_sql(self.client, self.warehouse_id, f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position")
        col_names = [c["column_name"] for c in cols]
        if key_column not in col_names:
            return {"error": f"Key column '{key_column}' not found in {fqn}"}

        # Read rows
        rows = execute_sql(self.client, self.warehouse_id, f"SELECT * FROM {fqn} LIMIT 1000")

        count = 0
        for row in rows:
            source_key = str(row.get(key_column, ""))
            if not source_key:
                continue
            attrs = {k: str(v) for k, v in row.items() if v is not None}
            source_record_id = str(uuid.uuid4())
            self.store.insert_source_record(source_record_id, None, entity_type, source_system, fqn, source_key, attrs, trust_score)
            count += 1

        logger.info(f"Ingested {count} source records from {fqn}")
        return {"source_table": fqn, "records_ingested": count, "entity_type": entity_type}

    # ---- Duplicate Detection ----

    def detect_duplicates(self, entity_type: str, auto_merge_threshold: float = 95.0, review_threshold: float = 80.0) -> dict:
        """Run matching rules against unmatched source records to find duplicates."""
        rules = self.store.get_rules(entity_type)
        enabled_rules = [r for r in rules if r.get("enabled")]
        if not enabled_rules:
            return {"error": "No enabled matching rules for this entity type", "pairs_found": 0}

        records = self.store.get_unmatched_source_records(entity_type, limit=500)
        if len(records) < 2:
            return {"pairs_found": 0, "auto_merged": 0, "for_review": 0}

        pairs_found = 0
        auto_merged = 0
        for_review = 0

        # Compare records using blocking + rules
        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                score, matched = self._compute_match_score(records[i], records[j], enabled_rules)
                if score >= review_threshold:
                    pair_id = str(uuid.uuid4())
                    name_a = records[i].get("attributes", {}).get("name", records[i].get("source_key", ""))
                    name_b = records[j].get("attributes", {}).get("name", records[j].get("source_key", ""))
                    if isinstance(name_a, dict):
                        name_a = str(name_a)
                    if isinstance(name_b, dict):
                        name_b = str(name_b)

                    status = "auto_merged" if score >= auto_merge_threshold else "pending"
                    self.store.insert_match_pair(pair_id, entity_type, records[i]["source_record_id"], records[j]["source_record_id"], str(name_a), str(name_b), score, matched)

                    if status == "auto_merged":
                        self._auto_merge(pair_id, records[i], records[j], score)
                        auto_merged += 1
                    else:
                        # Create stewardship task for manual review
                        task_id = str(uuid.uuid4())
                        priority = "high" if score >= 90 else "medium"
                        self.store.insert_task(task_id, "duplicate_review", entity_type, f"Potential duplicate ({score:.0f}% match): {name_a} ↔ {name_b}", priority, related_pair_id=pair_id)
                        for_review += 1

                    pairs_found += 1

        return {"pairs_found": pairs_found, "auto_merged": auto_merged, "for_review": for_review, "records_compared": len(records)}

    def _compute_match_score(self, record_a: dict, record_b: dict, rules: list) -> tuple[float, str]:
        """Compute weighted match score between two source records."""
        attrs_a = record_a.get("attributes", {})
        attrs_b = record_b.get("attributes", {})
        if isinstance(attrs_a, str):
            try:
                import json
                attrs_a = json.loads(attrs_a)
            except Exception:
                attrs_a = {}
        if isinstance(attrs_b, str):
            try:
                import json
                attrs_b = json.loads(attrs_b)
            except Exception:
                attrs_b = {}

        total_weight = 0
        weighted_score = 0
        matched_rules = []

        for rule in rules:
            field = rule["field"]
            match_type = rule["match_type"]
            weight = rule.get("weight", 1.0)
            threshold = rule.get("threshold", 0.0)

            val_a = attrs_a.get(field, "")
            val_b = attrs_b.get(field, "")

            if not val_a or not val_b:
                continue

            match_fn = MATCH_FUNCTIONS.get(match_type)
            if not match_fn:
                continue

            field_score = match_fn(val_a, val_b)
            if field_score >= threshold:
                weighted_score += field_score * weight
                total_weight += weight
                if field_score > 0:
                    matched_rules.append(f"{rule['name']}({field_score:.0%})")

        final_score = (weighted_score / total_weight * 100) if total_weight > 0 else 0
        return final_score, ", ".join(matched_rules)

    def _auto_merge(self, pair_id: str, record_a: dict, record_b: dict, score: float) -> None:
        """Auto-merge two records into a golden entity."""
        self.store.update_pair_status(pair_id, "auto_merged")
        entity_id = str(uuid.uuid4())
        attrs_a = record_a.get("attributes", {})
        attrs_b = record_b.get("attributes", {})
        if isinstance(attrs_a, str):
            attrs_a = {}
        if isinstance(attrs_b, str):
            attrs_b = {}

        # Simple survivorship: merge attributes, prefer non-empty from A then B
        merged = {}
        all_keys = set(list(attrs_a.keys()) + list(attrs_b.keys()))
        for k in all_keys:
            va = attrs_a.get(k, "")
            vb = attrs_b.get(k, "")
            merged[k] = va if va else vb

        display_name = merged.get("name", merged.get("display_name", record_a.get("source_key", "")))
        entity_type = record_a.get("entity_type", "")

        self.store.upsert_entity(entity_id, entity_type, str(display_name), merged, source_count=2, confidence_score=score / 100, created_by="auto_merge")
        self.store.link_source_to_entity(record_a["source_record_id"], entity_id)
        self.store.link_source_to_entity(record_b["source_record_id"], entity_id)

    # ---- Manual Merge & Split ----

    def merge_records(self, pair_id: str, strategy: str = "most_trusted") -> dict:
        """Manually merge a pair into a golden record."""
        pairs = self.store.get_match_pairs(status="pending")
        pair = next((p for p in pairs if p.get("pair_id") == pair_id), None)
        if not pair:
            return {"error": "Pair not found or already resolved"}

        record_a_id = pair["record_a_id"]
        record_b_id = pair["record_b_id"]

        sources_a = self.store.get_source_records(entity_id=None)
        record_a = next((r for r in sources_a if r.get("source_record_id") == record_a_id), None)
        record_b = next((r for r in sources_a if r.get("source_record_id") == record_b_id), None)

        if not record_a or not record_b:
            return {"error": "Source records not found"}

        self._auto_merge(pair_id, record_a, record_b, pair.get("match_score", 0))
        self.store.update_pair_status(pair_id, "merged", "manual")
        return {"pair_id": pair_id, "status": "merged", "strategy": strategy}

    def split_record(self, entity_id: str) -> dict:
        """Split a golden record back into individual source records."""
        sources = self.store.get_source_records(entity_id=entity_id)
        for src in sources:
            self.store.link_source_to_entity(src["source_record_id"], None)
        self.store.delete_entity(entity_id)
        return {"entity_id": entity_id, "split": True, "source_records_released": len(sources)}

    # ---- Stewardship ----

    def approve_task(self, task_id: str, user: str) -> dict:
        self.store.resolve_task(task_id, "approved", user)
        return {"task_id": task_id, "resolution": "approved"}

    def reject_task(self, task_id: str, user: str, reason: str = "") -> dict:
        self.store.resolve_task(task_id, f"rejected: {reason}" if reason else "rejected", user)
        return {"task_id": task_id, "resolution": "rejected"}

    # ---- Matching Rules ----

    def create_rule(self, entity_type: str, name: str, field: str, match_type: str, weight: float = 1.0, threshold: float = 0.8, enabled: bool = True) -> dict:
        rule_id = str(uuid.uuid4())
        self.store.upsert_rule(rule_id, entity_type, name, field, match_type, weight, threshold, enabled)
        return {"rule_id": rule_id, "name": name}

    def delete_rule(self, rule_id: str) -> dict:
        self.store.delete_rule(rule_id)
        return {"rule_id": rule_id, "deleted": True}

    # ---- Hierarchies ----

    def create_hierarchy(self, name: str, entity_type: str) -> dict:
        hierarchy_id = str(uuid.uuid4())
        root_node_id = str(uuid.uuid4())
        self.store.insert_hierarchy_node(hierarchy_id, name, entity_type, root_node_id, None, None, name, 0, f"/{name}")
        return {"hierarchy_id": hierarchy_id, "name": name, "root_node_id": root_node_id}

    def add_node(self, hierarchy_id: str, entity_id: str | None, label: str, parent_node_id: str, level: int = 1) -> dict:
        node_id = str(uuid.uuid4())
        self.store.insert_hierarchy_node(hierarchy_id, "", "", node_id, parent_node_id, entity_id, label, level, "")
        return {"node_id": node_id, "hierarchy_id": hierarchy_id}

    # ---- Dashboard ----

    def get_dashboard(self) -> dict:
        return self.store.get_dashboard_stats()
