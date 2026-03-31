/**
 * Auto-Visualization Heuristic Engine
 *
 * Recommends chart type + axis mappings based on column types and data shape.
 * Pure TypeScript — no network calls, runs in <1ms.
 */

export type ColType = "integer" | "long" | "double" | "float" | "decimal" | "string" | "boolean" | "date" | "timestamp" | "binary" | "array" | "map" | "struct" | "null";

export type ChartType = "bar" | "hbar" | "stacked" | "line" | "area" | "composed" | "scatter" | "pie" | "radar" | "funnel" | "treemap" | "map";

export interface VizRecommendation {
  chartType: ChartType;
  xCol: string;
  yCol: string;
  confidence: "high" | "medium" | "low";
  reason: string;
}

const TIME_NAME_PATTERNS = /^(date|time|timestamp|created|updated|modified|year|month|quarter|week|day|hour|minute|period|dt|ts|_at$|_date$|_time$|_ts$)/i;
const METRIC_NAME_PATTERNS = /^(count|sum|avg|total|amount|revenue|cost|price|quantity|rate|pct|percent|ratio|score|volume|sales|profit|margin|budget|spend|value|num_|cnt_)/i;

function isTimeColumn(colName: string, colType: ColType): boolean {
  if (colType === "date" || colType === "timestamp") return true;
  return TIME_NAME_PATTERNS.test(colName);
}

function isNumericType(colType: ColType): boolean {
  return ["integer", "long", "double", "float", "decimal"].includes(colType);
}

function isCategoryColumn(colName: string, colType: ColType, uniqueCount: number, rowCount: number): boolean {
  if (colType === "string" || colType === "boolean") {
    return uniqueCount < rowCount * 0.5;
  }
  return false;
}

function isMetricColumn(colName: string, colType: ColType): boolean {
  if (!isNumericType(colType)) return false;
  if (METRIC_NAME_PATTERNS.test(colName)) return true;
  return true; // any numeric column can serve as metric
}

function estimateCardinality(sampleRows: Record<string, any>[], colName: string): number {
  const values = new Set(sampleRows.map(r => String(r[colName] ?? "")));
  return values.size;
}

export function recommendVisualization(
  columns: string[],
  columnTypes: Record<string, ColType>,
  sampleRows: Record<string, any>[],
  rowCount: number,
): VizRecommendation {
  if (!columns.length || !sampleRows.length) {
    return { chartType: "bar", xCol: columns[0] || "", yCol: columns[1] || "", confidence: "low", reason: "No data to analyze" };
  }

  const timeCols = columns.filter(c => isTimeColumn(c, columnTypes[c]));
  const numCols = columns.filter(c => isNumericType(columnTypes[c]));
  const strCols = columns.filter(c => columnTypes[c] === "string" || columnTypes[c] === "boolean");
  const metricCols = numCols.filter(c => isMetricColumn(c, columnTypes[c]));

  // Prefer named metric columns, fall back to any numeric
  const bestMetric = metricCols.find(c => METRIC_NAME_PATTERNS.test(c)) || metricCols[0] || numCols[0];

  // Rule 1: Time + numeric -> line chart
  if (timeCols.length >= 1 && numCols.length >= 1) {
    const yCol = numCols.find(c => c !== timeCols[0]) || numCols[0];
    return { chartType: "line", xCol: timeCols[0], yCol, confidence: "high", reason: `Time series: ${timeCols[0]} over time` };
  }

  // Rule 2: Single string + single numeric -> bar or pie based on cardinality
  if (strCols.length >= 1 && numCols.length >= 1) {
    const catCol = strCols[0];
    const cardinality = estimateCardinality(sampleRows, catCol);

    // Rule 2a: Low cardinality -> pie
    if (cardinality <= 8 && cardinality >= 2) {
      return { chartType: "pie", xCol: catCol, yCol: bestMetric || numCols[0], confidence: "high", reason: `${cardinality} categories — ideal for pie chart` };
    }

    // Rule 2b: Medium cardinality -> bar
    if (cardinality <= 30) {
      return { chartType: "bar", xCol: catCol, yCol: bestMetric || numCols[0], confidence: "high", reason: `${cardinality} categories by ${bestMetric || numCols[0]}` };
    }

    // Rule 2c: High cardinality -> horizontal bar (more readable)
    if (cardinality > 30) {
      return { chartType: "hbar", xCol: catCol, yCol: bestMetric || numCols[0], confidence: "medium", reason: `${cardinality}+ categories — horizontal bar for readability` };
    }
  }

  // Rule 3: Two numerics only -> scatter
  if (numCols.length === 2 && strCols.length === 0 && timeCols.length === 0) {
    return { chartType: "scatter", xCol: numCols[0], yCol: numCols[1], confidence: "high", reason: `Correlation: ${numCols[0]} vs ${numCols[1]}` };
  }

  // Rule 4: One string + two numerics -> composed (bar + line)
  if (strCols.length >= 1 && numCols.length === 2) {
    return { chartType: "composed", xCol: strCols[0], yCol: numCols[0], confidence: "medium", reason: `Dual metrics: ${numCols[0]} and ${numCols[1]} by ${strCols[0]}` };
  }

  // Rule 5: One string + many numerics -> radar (small) or stacked (large)
  if (strCols.length >= 1 && numCols.length >= 3) {
    if (rowCount <= 10) {
      return { chartType: "radar", xCol: strCols[0], yCol: numCols[0], confidence: "medium", reason: `Multi-metric comparison across ${rowCount} items` };
    }
    return { chartType: "stacked", xCol: strCols[0], yCol: numCols[0], confidence: "medium", reason: `Stacked breakdown by ${strCols[0]}` };
  }

  // Rule 6: Two numerics with strings -> scatter
  if (numCols.length >= 2) {
    return { chartType: "scatter", xCol: numCols[0], yCol: numCols[1], confidence: "medium", reason: `Numeric correlation: ${numCols[0]} vs ${numCols[1]}` };
  }

  // Fallback: bar chart with first col as X, first numeric as Y
  return {
    chartType: "bar",
    xCol: columns[0],
    yCol: numCols[0] || columns[1] || columns[0],
    confidence: "low",
    reason: "Default visualization",
  };
}

/**
 * Build column-level summary statistics for AI explain.
 * Keeps payload small (< 5KB) by computing stats rather than sending all rows.
 */
export function buildColumnStats(
  columns: string[],
  columnTypes: Record<string, ColType>,
  rows: Record<string, any>[],
): Record<string, any>[] {
  return columns.map(col => {
    const values = rows.map(r => r[col]).filter(v => v != null);
    const nullCount = rows.length - values.length;
    const unique = new Set(values.map(String)).size;
    const stat: Record<string, any> = {
      column: col,
      type: columnTypes[col],
      nulls: nullCount,
      null_pct: rows.length ? +(nullCount / rows.length * 100).toFixed(1) : 0,
      unique,
    };

    if (isNumericType(columnTypes[col])) {
      const nums = values.map(Number).filter(n => !isNaN(n));
      if (nums.length) {
        stat.min = Math.min(...nums);
        stat.max = Math.max(...nums);
        stat.avg = +(nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2);
        const sorted = [...nums].sort((a, b) => a - b);
        stat.median = sorted[Math.floor(sorted.length / 2)];
      }
    } else {
      // Top 5 values by frequency
      const freq: Record<string, number> = {};
      values.forEach(v => { freq[String(v)] = (freq[String(v)] || 0) + 1; });
      stat.top_values = Object.entries(freq)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([value, count]) => ({ value, count }));
    }
    return stat;
  });
}
