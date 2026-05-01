---
title: Industry Templates
sidebar_label: Industry Templates
---

# Industry Templates

The Industry Templates page at `/mdm/templates` provides pre-built MDM models for common industries — banking, retail, healthcare, manufacturing. Each template ships with entity types, attributes, match rules, hierarchies, and reference data appropriate for that domain.

## Why templates

Building MDM from scratch is months of work. A template gets you to "matches and merges customers correctly out of the box" in an afternoon, then you customise from there.

## Available templates

| Template | Entities | Notable hierarchies |
|---|---|---|
| **Banking — Retail** | Customer, Account, Card, Address, Beneficiary | Customer → Accounts |
| **Banking — Commercial** | Legal Entity, Subsidiary, Account, Relationship Manager | Holding → Subsidiary |
| **Retail** | Customer, Product, SKU, Order, Loyalty Account | Category → Subcategory → SKU |
| **Healthcare — Patient** | Patient, Provider, Encounter, Insurance | Provider Network |
| **Healthcare — Clinical** | Patient, Episode, Diagnosis, Procedure | Care Pathway |
| **Manufacturing** | Customer, Product, BoM, Work Order, Supplier | BoM |

Each template includes:

- Entity type definitions with attributes and types
- Default match rules with sensible thresholds
- Default merge strategies per attribute
- Reference data sets (countries, currencies, ISO codes, NAICS, SIC, …)
- Sample hierarchies
- DQ rule pack tailored for the domain

## Apply a template

The wizard:

1. Pick template
2. Choose what to install — entities, rules, ref data, DQ rules (each toggleable)
3. Set the target catalog where MDM tables will live
4. Review and apply

```bash
POST /mdm/templates/{name}/apply
{
  "target_catalog": "mdm_warehouse",
  "components": ["entities", "match_rules", "reference_data", "dq_rules"]
}
```

After install, your MDM is ready to ingest source data with the bundled rules.

## Customisation

Templates are starting points — every component is editable post-install:

- Adjust attributes via [MDM Settings](mdm-settings.md)
- Tune match rules on [Match & Merge](match-merge.md)
- Modify hierarchies on [Hierarchies](hierarchies.md)
- Edit reference data on [Reference Data](reference-data.md)

## Customising templates

Author your own templates as YAML under `_clxs_artifacts/mdm-templates/`. Once present, they appear in the gallery on next reload. Templates are git-friendly.

## API

```bash
GET  /mdm/templates
GET  /mdm/templates/{name}
POST /mdm/templates/{name}/apply
```

## Related

- [Match & Merge](match-merge.md) — tunes rules from templates
- [Reference Data](reference-data.md) — bundled ref data lives here
- [MDM Overview](mdm.md)
