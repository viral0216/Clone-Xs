/**
 * Notebook Templates — predefined starter notebooks for common workflows.
 */

export interface NotebookTemplate {
  name: string;
  description: string;
  cells: { type: "sql" | "markdown"; content: string }[];
}

export const notebookTemplates: NotebookTemplate[] = [
  {
    name: "Explore Table",
    description: "Profile a table: schema, sample rows, stats, and distribution",
    cells: [
      { type: "markdown", content: "# Table Exploration\n\nReplace `catalog.schema.table` with your target table." },
      { type: "sql", content: "DESCRIBE TABLE EXTENDED {{catalog}}.{{schema}}.{{table}}" },
      { type: "sql", content: "SELECT * FROM {{catalog}}.{{schema}}.{{table}} LIMIT 100" },
      { type: "sql", content: "SELECT COUNT(*) AS row_count,\n  COUNT(DISTINCT *) AS distinct_rows\nFROM {{catalog}}.{{schema}}.{{table}}" },
      { type: "markdown", content: "## Column Stats\n\nCheck null rates and cardinality per column." },
      { type: "sql", content: "SELECT\n  COUNT(*) AS total_rows,\n  COUNT(*) - COUNT(col_name) AS null_count,\n  COUNT(DISTINCT col_name) AS distinct_count\nFROM {{catalog}}.{{schema}}.{{table}}" },
    ],
  },
  {
    name: "Data Quality Check",
    description: "Validate nulls, duplicates, freshness, and value ranges",
    cells: [
      { type: "markdown", content: "# Data Quality Assessment\n\nRun these checks against your target table." },
      { type: "sql", content: "-- Null check per column\nSELECT\n  COUNT(*) AS total,\n  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,\n  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS name_nulls,\n  SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS created_nulls\nFROM {{catalog}}.{{schema}}.{{table}}" },
      { type: "sql", content: "-- Duplicate check\nSELECT id, COUNT(*) AS cnt\nFROM {{catalog}}.{{schema}}.{{table}}\nGROUP BY id\nHAVING COUNT(*) > 1\nORDER BY cnt DESC\nLIMIT 20" },
      { type: "sql", content: "-- Freshness check\nSELECT\n  MAX(created_at) AS latest_record,\n  MIN(created_at) AS earliest_record,\n  DATEDIFF(CURRENT_DATE(), MAX(created_at)) AS days_stale\nFROM {{catalog}}.{{schema}}.{{table}}" },
      { type: "markdown", content: "## Summary\n\n- Add your findings here" },
    ],
  },
  {
    name: "Schema Comparison",
    description: "Compare schemas between two catalogs or tables",
    cells: [
      { type: "markdown", content: "# Schema Comparison\n\nCompare column definitions between source and target." },
      { type: "sql", content: "-- Source schema\nSELECT column_name, data_type, is_nullable\nFROM {{source_catalog}}.information_schema.columns\nWHERE table_schema = '{{schema}}' AND table_name = '{{table}}'\nORDER BY ordinal_position" },
      { type: "sql", content: "-- Target schema\nSELECT column_name, data_type, is_nullable\nFROM {{target_catalog}}.information_schema.columns\nWHERE table_schema = '{{schema}}' AND table_name = '{{table}}'\nORDER BY ordinal_position" },
      { type: "markdown", content: "## Differences\n\n- Note any schema drift here" },
    ],
  },
  {
    name: "Row Count Audit",
    description: "Compare row counts across all tables in a schema",
    cells: [
      { type: "markdown", content: "# Row Count Audit\n\nVerify table sizes across a schema." },
      { type: "sql", content: "SELECT table_name, table_type\nFROM {{catalog}}.information_schema.tables\nWHERE table_schema = '{{schema}}'\nORDER BY table_name" },
      { type: "sql", content: "-- Sample: count rows for a specific table\nSELECT '{{table}}' AS table_name, COUNT(*) AS row_count\nFROM {{catalog}}.{{schema}}.{{table}}" },
      { type: "markdown", content: "## Observations\n\n- Add row count analysis here" },
    ],
  },
  {
    name: "Cost Analysis",
    description: "Analyze storage and compute costs for a catalog",
    cells: [
      { type: "markdown", content: "# Cost & Storage Analysis\n\nReview table sizes and optimization opportunities." },
      { type: "sql", content: "-- Table sizes\nSELECT table_name,\n  data_source_format,\n  created,\n  last_altered\nFROM {{catalog}}.information_schema.tables\nWHERE table_schema = '{{schema}}'\nORDER BY last_altered DESC" },
      { type: "sql", content: "-- Storage detail for a specific table\nDESCRIBE DETAIL {{catalog}}.{{schema}}.{{table}}" },
      { type: "sql", content: "-- Table history (Delta)\nDESCRIBE HISTORY {{catalog}}.{{schema}}.{{table}} LIMIT 20" },
      { type: "markdown", content: "## Recommendations\n\n- Tables to OPTIMIZE\n- Tables to VACUUM\n- Unused tables to drop" },
    ],
  },
];
