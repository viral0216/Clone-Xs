import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Catalog Clone — full Unity Catalog parity',
    description: (
      <>
        Clone catalogs, schemas, or tables with permissions, tags, lineage,
        ownership, and Delta history. Cross-workspace via Delta Sharing.
        Iceberg sources accepted with a hidden-partition preflight.
      </>
    ),
  },
  {
    title: '6 Format Pairs — Delta ↔ Iceberg ↔ Parquet',
    description: (
      <>
        N×N in-place conversion with strategy dispatch (CONVERT TO DELTA,
        UniForm metadata, or CTAS+rename). Per-pair compatibility preflight
        refuses GENERATED columns and hidden Iceberg partitioning before any DDL.
      </>
    ),
  },
  {
    title: 'Demo Data Generator — 10 industries, medallion-ready',
    description: (
      <>
        Realistic, scaled demo data for IoT, finance, retail, healthcare, energy,
        and more. Bronze / Silver / Gold layers, DQ profiles, ML training labels,
        and four streaming destinations — Volume, Auto Loader Bronze,
        direct INSERT, or low-latency Zerobus.
      </>
    ),
  },
  {
    title: 'Data Quality — DQX integrated, 14 dimensions',
    description: (
      <>
        Rules, scorecards, anomalies, freshness, volume, trust scores,
        observability, and incidents. Powered by Databricks DQX with
        declarative YAML and per-table coverage tracking.
      </>
    ),
  },
  {
    title: 'Master Data Management — match, merge, govern',
    description: (
      <>
        Golden records, match-merge with negative-match overrides,
        hierarchies, profiling, scorecards, and a full MDM audit log.
        Stewardship workflow built in.
      </>
    ),
  },
  {
    title: 'FinOps — cost visibility end-to-end',
    description: (
      <>
        Workspace, query, job, and warehouse cost breakdowns. Storage
        optimisation recommendations, budget alerts, COPQ analysis, and
        compute-vs-storage trends from system tables.
      </>
    ),
  },
  {
    title: 'Compliance & Audit — DSAR, RTBF, RBAC',
    description: (
      <>
        Per-operation audit trails (clone_operations, convert_operations) with
        strategy fingerprints. Certifications, compliance frameworks (GDPR,
        HIPAA, SOC 2), consent flows, and PII detection.
      </>
    ),
  },
  {
    title: 'Lineage & Impact Analysis',
    description: (
      <>
        Trace data flows across catalogs and workspaces. Schema drift detection,
        view-dependency graphs, profiling histories, glossary terms, and
        impact analysis for every proposed change.
      </>
    ),
  },
  {
    title: 'Reconciliation & Validation',
    description: (
      <>
        Row, count, and aggregate reconciliation between source and target
        after every clone or convert. Diff-and-compare across formats,
        validation rule packs, and a rollback path when checks fail.
      </>
    ),
  },
  {
    title: 'Data Products — contracts, Marketplace, Sharing',
    description: (
      <>
        Productise tables: declare data contracts, publish to the Databricks
        Marketplace, share via Delta Sharing across regions, and track SLAs
        per consumer.
      </>
    ),
  },
  {
    title: 'Pipelines, CI/CD & Automation',
    description: (
      <>
        Schedule clones, converts, and DQ runs. CI/CD via GitHub Actions or
        Asset Bundles, Databricks Workflows, plugin system, reusable
        templates, and 10+ operator playbooks.
      </>
    ),
  },
  {
    title: 'AI Assistant & ML Asset Tracking',
    description: (
      <>
        Natural-language data-quality rules, an in-app AI assistant for catalog
        Q&amp;A, and ML asset tracking across model versions, registered
        features, and serving endpoints.
      </>
    ),
  },
];

function Feature({title, description}: Readonly<FeatureItem>) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props) => (
            <Feature key={props.title} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
