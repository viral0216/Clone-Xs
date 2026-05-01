---
title: Match & Merge
sidebar_label: Match & Merge
---

# Match & Merge

The Match & Merge page at `/mdm/match-merge` is where you author and run the matching rules and merge strategies that turn raw source records into [Golden Records](golden-records.md).

## Two phases

1. **Match** — score pairs of records on similarity, decide which are the same entity
2. **Merge** — combine matched records into a single golden record using a conflict-resolution strategy

Both phases run as Spark jobs on the configured warehouse.

## Match rules

A match rule defines:

- **Blocking key** — a coarse hash that limits comparisons (e.g. first letter of last name + zip prefix). Without blocking, matching is O(N²).
- **Comparators** — per-attribute similarity functions (`exact`, `levenshtein`, `jaro_winkler`, `phonetic`, `numeric_close`, `date_close`)
- **Weights** — contribution of each comparator to the overall similarity score
- **Threshold** — score above which two records are considered a match

```yaml
name: customer_match
entity_type: customer
blocking_key: lower(left(last_name, 3)) || left(zip, 3)
comparators:
  - { field: email,      method: exact,         weight: 50 }
  - { field: last_name,  method: jaro_winkler,  weight: 20 }
  - { field: first_name, method: jaro_winkler,  weight: 10 }
  - { field: phone,      method: exact,         weight: 15 }
  - { field: address,    method: levenshtein,   weight: 5 }
threshold: 80
```

## Merge strategies

When records match, merge resolves conflicts per attribute:

| Strategy | Notes |
|---|---|
| `latest` | Most-recently updated source value |
| `trusted_source` | Hard-coded source priority list |
| `most_complete` | Pick non-null over null |
| `longest_string` | For free-text fields like address |
| `mode` | Most common value across sources |
| `highest_confidence` | Use source-record confidence if scored |
| `manual` | Stewards resolve via [Stewardship](stewardship.md) |

A merge spec maps each attribute to a strategy:

```yaml
attributes:
  email: trusted_source
  phone: latest
  full_name: most_complete
  address: longest_string
```

## Preview before applying

The page has a **Preview** mode that runs match + merge against a sample (default 1000 records) and shows the resulting golden records. Inspect for surprises before running across the full source.

## Run

**Run match** triggers a full match job; **Run merge** consolidates matched groups into golden records. Both produce a result summary with counts (matched pairs, new goldens, merged-into-existing).

## Negative matches

Records explicitly marked **never match** ([Negative Match](negative-match.md)) are excluded — useful for known-different entities that the rules would otherwise consolidate.

## API

```bash
GET   /mdm/match-rules
POST  /mdm/match-rules
POST  /mdm/match/preview
POST  /mdm/match/run
POST  /mdm/merge/run
GET   /mdm/merge/runs                 # history
```

## Related

- [Golden Records](golden-records.md) — output of merge
- [Merge History](merge-history.md) — audit of merge runs
- [Negative Match](negative-match.md) — block specific consolidations
- [MDM Settings](mdm-settings.md) — global thresholds
