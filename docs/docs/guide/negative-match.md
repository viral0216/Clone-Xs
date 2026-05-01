---
title: Negative Match
sidebar_label: Negative Match
---

# Negative Match

The Negative Match page at `/mdm/negative-match` lists "do-not-match" rules — pairs or groups of records the matcher would otherwise consolidate but stewards have explicitly marked as different entities.

## Why negative match

Even well-tuned match rules produce false positives:

- Twins with the same name and address
- A customer and their adult child with the same email and phone
- Two business entities with very similar names but distinct legal structures
- A renamed entity that re-appears in source as a "new" record but mustn't merge with the legacy

Lowering the match threshold to avoid these breaks legitimate matches. Negative match is the surgical fix: keep rules tuned for the common case, blacklist exceptions explicitly.

## Rule types

| Type | Notes |
|---|---|
| **Pair** | Two specific entity IDs that must not merge |
| **Group** | A set of entity IDs that must remain separate from each other |
| **Pattern** | Rule predicate (e.g. *"records with same email but different last_name suffix never match"*) |

## Authoring

The most common path is via [Stewardship](stewardship.md): when a steward rejects a match, the case offers a checkbox to **Add to negative match**. Selecting it captures the pair as a permanent exception.

Manual authoring:

```yaml
type: pair
entity_a_id: ent_abc
entity_b_id: ent_xyz
reason: "Twin sisters - same name, address, phone, but distinct individuals"
created_by: steward@acme.com
```

Pattern rules:

```yaml
type: pattern
predicate: |
  a.email = b.email
  AND a.last_name = b.last_name
  AND (a.suffix IS NOT NULL OR b.suffix IS NOT NULL)
  AND a.suffix IS DISTINCT FROM b.suffix
reason: "Senior/Junior with shared email"
```

## Enforcement

When the matcher computes pairs, it checks each candidate match against the negative list. Matches blocked by negative match are recorded in the run log with the rule ID for audit.

## Review

The page lists all negative-match rules with:

- Entity IDs (clickable to [Golden Records](golden-records.md))
- Reason
- Created by + when
- Match attempts blocked (count)
- Status (active / inactive)

Stewards can deactivate a rule if circumstances change (e.g. it turns out the records really are duplicates).

## API

```bash
GET    /mdm/negative-match
POST   /mdm/negative-match
PATCH  /mdm/negative-match/{id}     # activate / deactivate
DELETE /mdm/negative-match/{id}
```

## Related

- [Stewardship](stewardship.md) — primary source of new rules
- [Match & Merge](match-merge.md) — where rules are enforced
- [MDM Audit Log](mdm-audit-log.md)
