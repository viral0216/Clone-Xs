---
title: Consent Management
sidebar_label: Consent
---

# Consent Management

The Consent page at `/mdm/consent` tracks customer consent flags and data-usage permissions across MDM golden records. It's where GDPR / CCPA / HIPAA-style consent obligations are recorded and enforced.

## Consent dimensions

Each customer (or other consenting entity) has consent flags across multiple dimensions:

| Dimension | Examples |
|---|---|
| **Marketing** | email_marketing, sms_marketing, push_notifications |
| **Analytics** | behavioral_tracking, anonymous_analytics |
| **Sharing** | share_with_partners, share_with_affiliates |
| **Profiling** | automated_decision_making, personalised_offers |
| **Retention** | retain_after_relationship_end |

Each flag has a value (`granted`, `denied`, `unknown`), a source (where the consent came from), a timestamp, and an expiry.

## Source of consent

Where the consent was recorded:

- **Web form** — at signup, with versioned T&Cs
- **Phone call** — captured by agent
- **Letter / email** — opt-out written confirmation
- **Implicit** — derived from regulation (e.g. existing-customer marketing exemption)
- **Imported** — from legacy CRM during onboarding

The provenance is critical for audit defence.

## Page sections

- **Customer consent record** — drill in by entity ID; see all flags, history, sources
- **Consent inventory** — bulk view by flag (e.g. *"all customers with email_marketing = granted"*)
- **Recent changes** — last 30 days of consent updates
- **Expiring soon** — consents within 30 days of expiry

## Updating consent

```bash
POST /mdm/consent/{entity_id}
{
  "flag": "email_marketing",
  "value": "denied",
  "source": "web_form",
  "source_ref": "form_submission_2026_04_30_18a",
  "occurred_at": "2026-04-30T14:22:00Z",
  "actor": "customer"
}
```

The page records the prior value and links the change in the audit log.

## Enforcement

Consent flags are referenced by:

- Marketing automation (only sends to `email_marketing = granted`)
- Analytics pipelines (apply `behavioral_tracking` filter)
- Data sharing ([Delta Sharing](delta-sharing.md) skips records denying share_with_partners)
- [RTBF](rtbf.md) — denied retention triggers erasure flow

The consent service is the single source of truth — every downstream system queries it rather than caching consent locally.

## Audit

Every consent change is logged with:

- Before / after values
- Source + source_ref
- Actor (customer / employee / system)
- Timestamp

Retention is regulatory-driven (e.g. GDPR audit-trail retention). Configure in `clxs.yaml`.

## API

```bash
GET   /mdm/consent/{entity_id}
GET   /mdm/consent/inventory?flag=email_marketing&value=granted
POST  /mdm/consent/{entity_id}
GET   /mdm/consent/{entity_id}/history
```

## Related

- [RTBF](rtbf.md) — right-to-be-forgotten interplay
- [DSAR](dsar.md) — data subject access requests
- [Compliance Frameworks](compliance-frameworks.md) — regulatory mapping
