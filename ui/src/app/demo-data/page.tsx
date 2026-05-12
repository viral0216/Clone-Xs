// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine,
} from "recharts";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import FieldLabel, { InfoDot } from "@/components/FieldLabel";
import { useCurrency } from "@/hooks/useSettings";
import { useStreamingEmit, useStreamingStop, useVolumes, useStreamingSchedule, useDemoCatalogs, useDemoCatalogDrop } from "@/hooks/useApi";
import { useDurableJob } from "@/hooks/useDurableJob";
import {
  Database, Loader2, CheckCircle2, XCircle, Play, RefreshCw, Clock,
  ChevronDown, ChevronUp, Info, Zap, DollarSign, Trash2, ExternalLink,
  ClipboardCopy, Check, Download, Radio, StopCircle, Calendar, Settings2,
  Copy, FileText, Image as ImageIcon, BookOpen, ScrollText, Code2, Camera,
} from "lucide-react";
import DocumentsTab from "./DocumentsTab";
import MediaTab from "./MediaTab";
import KnowledgeTab from "./KnowledgeTab";
import LogsTab from "./LogsTab";
import CodeTab from "./CodeTab";
import LiveCaptureTab from "./LiveCaptureTab";

// Small helper used by the streaming card's copy buttons. Falls back
// gracefully on browsers without `navigator.clipboard` (older Safari,
// non-secure contexts) so the click still feels responsive.
async function copyToClipboard(text: string, label = "Copied") {
  try {
    await navigator.clipboard.writeText(text);
    toast.success(label);
  } catch {
    toast.error("Could not copy to clipboard");
  }
}

const INDUSTRIES = ["healthcare", "financial", "retail", "telecom", "manufacturing", "energy", "education", "real_estate", "logistics", "insurance"] as const;

const SCALE_OPTIONS = [
  { value: "0.01", label: "0.01 — Test (~10M rows)" },
  { value: "0.1", label: "0.1 — Small (~100M rows)" },
  { value: "0.5", label: "0.5 — Medium (~500M rows)" },
  { value: "1.0", label: "1.0 — Full (~1B rows)" },
];

// Compact number formatter for chart axes / tooltips. Turns 3,000,000
// into "3M" — the throughput chart Y-axis is unreadable without it
// once batch sizes pass ~10K.
const fmtN = (n: number): string => {
  if (n == null || Number.isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(abs >= 1e10 ? 0 : 1).replace(/\.0$/, "") + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(abs >= 1e7 ? 0 : 1).replace(/\.0$/, "") + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(abs >= 1e4 ? 0 : 1).replace(/\.0$/, "") + "K";
  return String(Math.round(n));
};

// Streaming-emit performance presets. Each preset bundles a destination
// + cadence combo that targets a different throughput tier — clicking
// one sets all four state vars (with clamping to streamLimits if the
// admin has narrowed bounds in Settings). "Custom" is auto-selected
// whenever the live values don't match any preset; clicking it is a
// no-op (it's only there to indicate the current state).
//
// Throughput numbers in the descriptions are typical for a small/medium
// DBSQL Serverless warehouse — actual numbers vary by warehouse size,
// network, and event-shape complexity.
type StreamPresetId = "demo" | "direct_small" | "bulk_files" | "zerobus" | "custom";
type StreamDestinationVal = "volume" | "volume_bronze" | "direct_table" | "zerobus";
const STREAMING_PRESETS: {
  id: StreamPresetId;
  label: string;
  desc: string;
  destination: StreamDestinationVal;
  events_per_batch: number;
  interval_seconds: number;
  total_duration_seconds: number;
  requiresZerobus?: boolean;
}[] = [
  {
    id: "demo",
    label: "Demo (default)",
    desc: "~5K rows/s — fastest to start, good for screen-shares",
    destination: "volume_bronze",
    events_per_batch: 100,
    interval_seconds: 5,
    total_duration_seconds: 60,
  },
  {
    id: "direct_small",
    label: "Direct (small batches)",
    desc: "~30–50K rows/s — INSERT VALUES tuned for steady cadence",
    destination: "direct_table",
    events_per_batch: 50000,
    interval_seconds: 1,
    total_duration_seconds: 300,
  },
  {
    id: "bulk_files",
    label: "Bulk files",
    desc: "~100–500K rows/s — large JSON files + Auto Loader",
    destination: "volume_bronze",
    events_per_batch: 100000,
    interval_seconds: 2,
    total_duration_seconds: 300,
  },
  {
    id: "zerobus",
    label: "Streaming (Zerobus)",
    desc: "~100K–1M+ rows/s — gRPC direct append (Premium tier)",
    destination: "zerobus",
    events_per_batch: 1000000,
    interval_seconds: 5,
    total_duration_seconds: 600,
    requiresZerobus: true,
  },
];

const INDUSTRY_TABLES: Record<string, { name: string; rows: number }[]> = {
  healthcare: [
    { name: "claims", rows: 100_000_000 }, { name: "encounters", rows: 50_000_000 }, { name: "prescriptions", rows: 30_000_000 },
    { name: "lab_results", rows: 5_000_000 }, { name: "vital_signs", rows: 5_000_000 }, { name: "patients", rows: 1_000_000 },
    { name: "providers", rows: 1_000_000 }, { name: "facilities", rows: 1_000_000 }, { name: "insurance_plans", rows: 1_000_000 },
    { name: "drug_catalog", rows: 1_000_000 }, { name: "diagnosis_codes", rows: 500_000 }, { name: "procedure_codes", rows: 500_000 },
    { name: "pharmacies", rows: 200_000 }, { name: "specialties", rows: 100_000 }, { name: "claim_lines", rows: 500_000 },
    { name: "referrals", rows: 300_000 }, { name: "appointments", rows: 500_000 }, { name: "allergies", rows: 200_000 },
    { name: "immunizations", rows: 200_000 }, { name: "billing_adjustments", rows: 500_000 },
  ],
  financial: [
    { name: "transactions", rows: 100_000_000 }, { name: "card_events", rows: 50_000_000 }, { name: "loan_payments", rows: 30_000_000 },
    { name: "wire_transfers", rows: 5_000_000 }, { name: "trading_orders", rows: 5_000_000 }, { name: "accounts", rows: 1_000_000 },
    { name: "customers", rows: 1_000_000 }, { name: "branches", rows: 1_000_000 }, { name: "cards", rows: 1_000_000 },
    { name: "loans", rows: 1_000_000 }, { name: "fraud_alerts", rows: 500_000 }, { name: "merchants", rows: 500_000 },
    { name: "interest_rates", rows: 200_000 }, { name: "compliance_events", rows: 300_000 }, { name: "credit_scores", rows: 300_000 },
    { name: "atm_transactions", rows: 500_000 }, { name: "account_statements", rows: 200_000 }, { name: "beneficiaries", rows: 200_000 },
    { name: "exchange_rates", rows: 100_000 }, { name: "risk_assessments", rows: 100_000 },
  ],
  retail: [
    { name: "order_items", rows: 100_000_000 }, { name: "clickstream", rows: 50_000_000 }, { name: "reviews", rows: 30_000_000 },
    { name: "inventory_movements", rows: 5_000_000 }, { name: "promotions_usage", rows: 5_000_000 }, { name: "customers_retail", rows: 1_000_000 },
    { name: "products", rows: 1_000_000 }, { name: "stores", rows: 1_000_000 }, { name: "orders", rows: 1_000_000 },
    { name: "suppliers", rows: 1_000_000 }, { name: "categories", rows: 500_000 }, { name: "warehouses", rows: 500_000 },
    { name: "promotions", rows: 200_000 }, { name: "loyalty_members", rows: 300_000 }, { name: "returns", rows: 300_000 },
    { name: "shipments", rows: 500_000 }, { name: "price_history", rows: 200_000 }, { name: "gift_cards", rows: 200_000 },
    { name: "coupons", rows: 100_000 }, { name: "wishlists", rows: 100_000 },
  ],
  telecom: [
    { name: "cdr_records", rows: 100_000_000 }, { name: "data_usage", rows: 50_000_000 }, { name: "sms_records", rows: 30_000_000 },
    { name: "network_events", rows: 5_000_000 }, { name: "trouble_tickets", rows: 5_000_000 }, { name: "subscribers", rows: 1_000_000 },
    { name: "plans_telecom", rows: 1_000_000 }, { name: "devices", rows: 1_000_000 }, { name: "cell_towers", rows: 1_000_000 },
    { name: "invoices_telecom", rows: 1_000_000 }, { name: "porting_requests", rows: 500_000 }, { name: "service_orders", rows: 500_000 },
    { name: "roaming_events", rows: 200_000 }, { name: "bundle_subscriptions", rows: 300_000 }, { name: "customer_interactions", rows: 300_000 },
    { name: "churn_predictions", rows: 500_000 }, { name: "coverage_areas", rows: 200_000 }, { name: "equipment_inventory", rows: 200_000 },
    { name: "rate_plans", rows: 100_000 }, { name: "promotions_telecom", rows: 100_000 },
  ],
  manufacturing: [
    { name: "sensor_readings", rows: 100_000_000 }, { name: "production_events", rows: 50_000_000 }, { name: "quality_checks", rows: 30_000_000 },
    { name: "maintenance_logs", rows: 5_000_000 }, { name: "defect_reports", rows: 5_000_000 }, { name: "work_orders", rows: 1_000_000 },
    { name: "machines", rows: 1_000_000 }, { name: "parts_inventory", rows: 1_000_000 }, { name: "production_lines", rows: 1_000_000 },
    { name: "employees_mfg", rows: 1_000_000 }, { name: "suppliers_mfg", rows: 500_000 }, { name: "raw_materials", rows: 500_000 },
    { name: "shift_schedules", rows: 200_000 }, { name: "safety_incidents", rows: 300_000 }, { name: "energy_consumption", rows: 300_000 },
    { name: "product_specs", rows: 500_000 }, { name: "shipping_orders", rows: 200_000 }, { name: "bill_of_materials", rows: 200_000 },
    { name: "tool_inventory", rows: 100_000 }, { name: "calibration_records", rows: 100_000 },
  ],
  energy: [
    { name: "meter_readings", rows: 100_000_000 }, { name: "grid_events", rows: 50_000_000 }, { name: "outage_reports", rows: 30_000_000 },
    { name: "generation_data", rows: 5_000_000 }, { name: "emissions_data", rows: 5_000_000 }, { name: "customers_energy", rows: 1_000_000 },
    { name: "power_plants", rows: 1_000_000 }, { name: "substations", rows: 500_000 }, { name: "transformers", rows: 1_000_000 },
    { name: "service_points", rows: 1_000_000 }, { name: "rate_schedules", rows: 500_000 }, { name: "billing_energy", rows: 500_000 },
    { name: "demand_forecasts", rows: 200_000 }, { name: "renewable_assets", rows: 300_000 }, { name: "maintenance_energy", rows: 300_000 },
    { name: "regulatory_filings", rows: 500_000 }, { name: "weather_data", rows: 200_000 }, { name: "load_profiles", rows: 200_000 },
    { name: "interconnections", rows: 100_000 }, { name: "carbon_credits", rows: 100_000 },
  ],
  education: [
    { name: "enrollments", rows: 100_000_000 }, { name: "grades", rows: 50_000_000 }, { name: "attendance", rows: 30_000_000 },
    { name: "assignments", rows: 5_000_000 }, { name: "course_evaluations", rows: 5_000_000 }, { name: "students", rows: 1_000_000 },
    { name: "courses", rows: 1_000_000 }, { name: "instructors", rows: 500_000 }, { name: "financial_aid", rows: 500_000 },
    { name: "facilities_edu", rows: 300_000 }, { name: "libraries", rows: 500_000 }, { name: "research_grants", rows: 500_000 },
    { name: "alumni", rows: 200_000 }, { name: "departments", rows: 100_000 }, { name: "programs", rows: 300_000 },
    { name: "scholarships", rows: 500_000 }, { name: "transcripts", rows: 200_000 }, { name: "campus_events", rows: 200_000 },
    { name: "student_orgs", rows: 100_000 }, { name: "housing", rows: 100_000 },
  ],
  real_estate: [
    { name: "listings", rows: 100_000_000 }, { name: "transactions_re", rows: 50_000_000 }, { name: "showings", rows: 30_000_000 },
    { name: "appraisals", rows: 5_000_000 }, { name: "inspections", rows: 5_000_000 }, { name: "agents", rows: 1_000_000 },
    { name: "properties", rows: 1_000_000 }, { name: "neighborhoods", rows: 500_000 }, { name: "mortgages_re", rows: 1_000_000 },
    { name: "buyers", rows: 1_000_000 }, { name: "sellers", rows: 500_000 }, { name: "brokerages", rows: 500_000 },
    { name: "open_houses", rows: 200_000 }, { name: "market_reports", rows: 300_000 }, { name: "zoning_data", rows: 300_000 },
    { name: "tax_assessments", rows: 500_000 }, { name: "permits", rows: 200_000 }, { name: "hoa_data", rows: 200_000 },
    { name: "rental_listings", rows: 100_000 }, { name: "property_photos", rows: 100_000 },
  ],
  logistics: [
    { name: "shipment_events", rows: 100_000_000 }, { name: "tracking_updates", rows: 50_000_000 }, { name: "delivery_attempts", rows: 30_000_000 },
    { name: "route_segments", rows: 5_000_000 }, { name: "customs_declarations", rows: 5_000_000 }, { name: "drivers", rows: 1_000_000 },
    { name: "vehicles", rows: 1_000_000 }, { name: "warehouses_lg", rows: 1_000_000 }, { name: "shipments_lg", rows: 1_000_000 },
    { name: "customers_lg", rows: 1_000_000 }, { name: "carriers", rows: 500_000 }, { name: "routes", rows: 500_000 },
    { name: "fuel_logs", rows: 200_000 }, { name: "dock_schedules", rows: 300_000 }, { name: "inventory_lg", rows: 300_000 },
    { name: "freight_rates", rows: 500_000 }, { name: "claims_lg", rows: 200_000 }, { name: "packaging_types", rows: 200_000 },
    { name: "service_levels", rows: 100_000 }, { name: "zones", rows: 100_000 },
  ],
  insurance: [
    { name: "claims_ins", rows: 100_000_000 }, { name: "premium_payments", rows: 50_000_000 }, { name: "underwriting_events", rows: 30_000_000 },
    { name: "risk_assessments_ins", rows: 5_000_000 }, { name: "policy_changes", rows: 5_000_000 }, { name: "policyholders", rows: 1_000_000 },
    { name: "agents_ins", rows: 1_000_000 }, { name: "policies", rows: 1_000_000 }, { name: "coverage_types", rows: 1_000_000 },
    { name: "beneficiaries_ins", rows: 1_000_000 }, { name: "adjusters", rows: 500_000 }, { name: "reinsurance", rows: 500_000 },
    { name: "catastrophe_events", rows: 100_000 }, { name: "fraud_indicators", rows: 300_000 }, { name: "actuarial_tables", rows: 300_000 },
    { name: "regulatory_filings_ins", rows: 500_000 }, { name: "commissions", rows: 200_000 }, { name: "loss_reserves", rows: 200_000 },
    { name: "product_lines", rows: 100_000 }, { name: "agency_contracts", rows: 100_000 },
  ],
};

// Numbered step in the Zerobus credentials block. Renders a circle
// (number when pending, green check when done) on the left, the title
// + optional/done badges on the right, and the step body underneath.
// Used only by the Zerobus form — kept here at module scope so the
// main page component doesn't grow another render helper.
function ZerobusStep({
  number,
  title,
  done,
  optional = false,
  children,
}: Readonly<{
  number: number;
  title: string;
  done: boolean;
  optional?: boolean;
  children: React.ReactNode;
}>) {
  return (
    <div className="flex gap-3">
      <div
        className={`shrink-0 w-7 h-7 rounded-full flex items-center justify-center text-xs font-semibold ${
          done
            ? "bg-emerald-500 text-white border border-emerald-500"
            : "bg-background text-muted-foreground border border-border"
        }`}
        aria-label={done ? `Step ${number} done` : `Step ${number}`}
      >
        {done ? <CheckCircle2 className="h-4 w-4" /> : number}
      </div>
      <div className="flex-1 space-y-1.5 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-medium text-[13px]">{title}</span>
          {optional && (
            <Badge variant="outline" className="text-[10px]">
              Optional
            </Badge>
          )}
          {done && !optional && (
            <Badge
              variant="outline"
              className="text-[10px] border-emerald-300 text-emerald-700 bg-emerald-50"
            >
              Done
            </Badge>
          )}
        </div>
        {children}
      </div>
    </div>
  );
}

function statusBadge(status: string) {
  switch (status?.toLowerCase()) {
    case "completed":
      return <Badge className="bg-muted/40 text-foreground text-xs">COMPLETED</Badge>;
    case "running":
      return <Badge className="bg-muted/50 text-foreground text-xs">RUNNING</Badge>;
    case "queued":
      return <Badge className="bg-muted/40 text-foreground text-xs">QUEUED</Badge>;
    case "failed":
      return <Badge variant="destructive" className="text-xs">FAILED</Badge>;
    default:
      return <Badge variant="outline" className="text-xs">{status?.toUpperCase()}</Badge>;
  }
}

export default function DemoDataPage() {
  // Form state
  const [catalogName, setCatalogName] = useState("");
  const [selectedIndustries, setSelectedIndustries] = useState<string[]>([...INDUSTRIES]);
  const [scaleFactor, setScaleFactor] = useState("0.01");
  const [owner, setOwner] = useState("");
  const [storageLocation, setStorageLocation] = useState("");
  const [startDate, setStartDate] = useState("2020-01-01");
  const [endDate, setEndDate] = useState("2025-01-01");
  const [destCatalog, setDestCatalog] = useState("");
  const [dropExisting, setDropExisting] = useState(false);
  const [medallion, setMedallion] = useState(true);
  const [ucBestPractices, setUcBestPractices] = useState(true);
  const [createFunctions, setCreateFunctions] = useState(true);
  const [createVolumes, setCreateVolumes] = useState(true);
  // schema_only: when true, the backend creates DDL (catalog/schemas/tables/
  // views/UDFs/volumes) without running any INSERT statements. Cuts a 60-min
  // generation down to seconds — perfect for verifying DDL templates, CI
  // smoke runs, or YAML custom-industry validation.
  const [schemaOnly, setSchemaOnly] = useState(false);
  // Theme 1 (Realism): when on, the backend rewrites the small static
  // name/email/phone pools embedded in INSERT expressions to sample from
  // Faker-generated, locale-aware pools. `seed` makes the output
  // deterministic across runs (handy for screenshot demos).
  const [realisticData, setRealisticData] = useState(false);
  const [locale, setLocale] = useState("en_US");
  const [seed, setSeed] = useState<string>("");
  // Theme 2 (DQ profiles + ML labels). dq_profile is a named bundle of
  // null/dup/outlier rates; anomaly_rate is the positive-class rate for
  // labeled training columns (is_fraud / churn_risk / is_anomaly).
  const [dqProfile, setDqProfile] = useState<"clean" | "realistic" | "dirty">("realistic");
  const [anomalyRate, setAnomalyRate] = useState<number>(0.02);
  const [injectAnomalies, setInjectAnomalies] = useState(true);
  // Data modeling pattern overlay. "flat" preserves today's behaviour;
  // "star_schema" additionally generates `<industry>_star` schemas with
  // fct_/dim_ tables following DBT-style naming (DV2/OBT/Snowflake later).
  const [dataModel, setDataModel] = useState<"flat" | "star_schema">("flat");

  // Preview state
  const [previewOpen, setPreviewOpen] = useState(true);
  const [industryDetailOpen, setIndustryDetailOpen] = useState(false);
  // Theme 4 — server-computed per-industry preview. Lazily fetched from
  // POST /generate/demo-data/preview when the user clicks "Refresh"
  // (default off so a stale form doesn't keep hammering the API).
  const [livePreview, setLivePreview] = useState<{
    per_industry: Array<{ industry: string; tables: number; rows: number; estimated_bytes: number; estimated_duration_seconds: number }>;
    total_rows: number;
    total_gb: number;
    estimated_duration_seconds: number;
    estimated_cost_usd: { monthly_storage: number; one_time_compute: number; first_month_total: number };
  } | null>(null);
  const [livePreviewLoading, setLivePreviewLoading] = useState(false);

  const fetchLivePreview = async () => {
    if (selectedIndustries.length === 0) return;
    setLivePreviewLoading(true);
    try {
      const res = await api.post("/generate/demo-data/preview", {
        catalog_name: catalogName.trim() || "demo_preview",
        industries: selectedIndustries,
        scale_factor: Number.parseFloat(scaleFactor),
        schema_only: schemaOnly,
      });
      setLivePreview(res);
    } catch (e: any) {
      toast.error(`Preview failed: ${e?.message || e}`);
    } finally {
      setLivePreviewLoading(false);
    }
  };
  const [expandedIndustries, setExpandedIndustries] = useState<Set<string>>(new Set());
  const [cleanupLoading, setCleanupLoading] = useState(false);

  // Template presets
  const applyPreset = (preset: "quick" | "sales" | "full") => {
    switch (preset) {
      case "quick":
        setCatalogName("demo_quick");
        setSelectedIndustries(["healthcare"]);
        setScaleFactor("0.01");
        setMedallion(false);
        break;
      case "sales":
        setCatalogName("demo_sales");
        setSelectedIndustries(["healthcare", "financial", "retail"]);
        setScaleFactor("0.1");
        setMedallion(true);
        break;
      case "full":
        setCatalogName("demo_full");
        setSelectedIndustries([...INDUSTRIES]);
        setScaleFactor("1.0");
        setMedallion(true);
        break;
    }
  };

  // Computed preview stats
  const industriesCount = selectedIndustries.length;
  const scale = parseFloat(scaleFactor);
  const schemasCount = medallion
    ? industriesCount * 4 + 1 // base + bronze + silver + gold per industry + cross_industry
    : industriesCount + 1; // base per industry + cross_industry
  const estimatedTables = industriesCount * 20;
  const estimatedViews = industriesCount * 20;
  const estimatedUdfs = industriesCount * 20;
  const estimatedRows = industriesCount * 200_000_000 * scale;
  const [storagePricePerGb, setStoragePricePerGb] = useState(0.023);
  const { symbol: currSymbol } = useCurrency();

  // Load pricing from backend config
  useEffect(() => {
    api.get<any>("/config").then((config) => {
      if (config?.price_per_gb != null) setStoragePricePerGb(config.price_per_gb);
    }).catch(() => {
      try {
        const saved = localStorage.getItem("clxs-price-per-gb");
        if (saved) setStoragePricePerGb(parseFloat(saved) || 0.023);
      } catch {}
    });
  }, []);
  const estimatedStorageBytes = estimatedRows * 100; // 100 bytes avg row
  const estimatedStorageGb = estimatedStorageBytes / (1024 * 1024 * 1024);
  const estimatedStorageCost = estimatedStorageGb * storagePricePerGb;
  const estimatedDbus = industriesCount * scale * 50;

  const formatNumber = (n: number) => {
    if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toFixed(0);
  };

  // Job state — durable across page navigation. The hook polls the server,
  // persists job_id + latest server dict in JobContext, and reconnects to an
  // in-flight job on remount so progress/logs resume mid-run.
  const batchJob = useDurableJob({
    key: "demo-data-batch",
    pollUrl: (id) => `/clone/${id}`,
    pollInterval: 2000,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
    notificationTitle: "Demo data ready",
    onComplete: (d) => {
      if (d.status === "completed") toast.success("Demo data generated successfully");
      else toast.error(d.error || "Demo data generation failed");
    },
  });
  const jobId = batchJob.jobId;
  const job = batchJob.entry?.data ?? null;
  const [submitting, setSubmitting] = useState(false);
  const logsEndRef = useRef<HTMLDivElement | null>(null);

  // Top-level "which generator?" switch. The page houses two distinct
  // generators (batch catalog + streaming events); each gets its own
  // tab so users aren't scrolling past inapplicable controls.
  // Persisted to sessionStorage so refresh keeps the user where they
  // were.
  const [activeGenTab, setActiveGenTab] = useState<"batch" | "streaming" | "documents" | "media" | "knowledge" | "logs" | "code" | "capture" | "manage">(() => {
    try {
      const v = sessionStorage.getItem("clxs-demo-gen-tab");
      if (v === "batch" || v === "streaming" || v === "documents" || v === "media" || v === "knowledge" || v === "logs" || v === "code" || v === "capture" || v === "manage") return v;
    } catch {}
    return "batch";
  });

  // Manage Catalogs tab state — typed-confirm modal sits above the
  // listing. The modal asks the user to type the catalog name to
  // arm the destructive Confirm button (typed-confirm pattern, not
  // a window.confirm() — we want the higher safety bar here than the
  // existing Batch-tab inline delete).
  const [manageDemoOnly, setManageDemoOnly] = useState(false);
  const [dropModalCatalog, setDropModalCatalog] = useState<string | null>(null);
  const [dropModalTyped, setDropModalTyped] = useState("");
  const demoCatalogsQuery = useDemoCatalogs(manageDemoOnly);
  const demoCatalogDrop = useDemoCatalogDrop();

  // Schedule-streaming modal state — opened by the "Schedule on
  // Databricks" button on the Streaming tab. All fields prefilled
  // from sensible defaults; user only needs to click submit.
  const [scheduleModalOpen, setScheduleModalOpen] = useState(false);
  const [scheduleName, setScheduleName] = useState("");
  const [scheduleCron, setScheduleCron] = useState("0 */5 * * * ?");
  const [scheduleTimezone, setScheduleTimezone] = useState("UTC");
  const [scheduleUseServerless, setScheduleUseServerless] = useState(true);
  const [scheduleNotebookPath, setScheduleNotebookPath] = useState("");
  const [scheduleResult, setScheduleResult] = useState<any>(null);
  const streamingSchedule = useStreamingSchedule();

  // Streaming-emission card state. Independent from the batch form
  // above — has its own catalog/schema/profile inputs and its own
  // job lifecycle. Re-uses the same /api/jobs/{id} polling endpoint
  // by tracking `streamingJobId` and pulling the same job dict.
  const [streamCatalog, setStreamCatalog] = useState("");
  const [streamSchema, setStreamSchema] = useState("iot");
  // UC Volume the runner writes JSON batches into. Default matches the
  // legacy hardcoded name so existing demos keep working; users can pick
  // an existing volume from the suggestions list or type a new name
  // (the runner CREATE VOLUME IF NOT EXISTS the chosen name).
  const [streamVolume, setStreamVolume] = useState("events_volume");
  // "Custom mode" flags for the three target-location selects. Tracked
  // explicitly because the value alone can't distinguish "user picked
  // Custom and is partway through typing" from "user is between picks
  // (value temporarily empty)". The runner CREATE … IF NOT EXISTS
  // catalog/schema/volume so any chosen name works whether it exists
  // already or not.
  const [streamCatalogCustom, setStreamCatalogCustom] = useState(false);
  const [streamSchemaCustom, setStreamSchemaCustom] = useState(false);
  const [streamVolumeCustom, setStreamVolumeCustom] = useState(false);
  const volumesQuery = useVolumes();
  const catalogsQuery = useQuery<string[]>({
    queryKey: ["catalogs"],
    queryFn: () => api.get("/catalogs"),
    staleTime: 1000 * 60 * 10,
  });
  const schemasQuery = useQuery<string[]>({
    queryKey: ["schemas", streamCatalog],
    queryFn: () => api.get(`/catalogs/${streamCatalog}/schemas`),
    // Only fetch when we have an existing catalog selected. If the user
    // is in "Custom catalog" mode the catalog doesn't exist yet, so
    // there are no schemas to enumerate.
    enabled: !!streamCatalog && !streamCatalogCustom,
    staleTime: 1000 * 60 * 5,
  });
  const [streamProfile, setStreamProfile] = useState<
    | "generic_sensor"
    | "industrial_machine"
    | "car_obd2"
    | "smart_meter"
    | "wearable_health"
    | "pos_terminal"
    | "wind_turbine"
    | "atm_transaction"
    | "server_metrics"
    | "clickstream"
  >("generic_sensor");
  const [streamEventsPerBatch, setStreamEventsPerBatch] = useState(100);
  const [streamIntervalSeconds, setStreamIntervalSeconds] = useState(5);
  const [streamDurationSeconds, setStreamDurationSeconds] = useState(60);
  // Form bounds fetched from /generate/demo-data/streaming/limits.
  // Sourced from `streaming_limits` in clone_config.yaml — workspace
  // admins can widen / narrow the inputs without a code change. Falls
  // back to the values previously hardcoded in the JSX so the page
  // stays usable if the API is unreachable.
  type StreamLimit = { default: number; min: number; max: number };
  type StreamLimits = {
    events_per_batch: StreamLimit;
    interval_seconds: StreamLimit;
    total_duration_seconds: StreamLimit;
  };
  const [streamLimits, setStreamLimits] = useState<StreamLimits>({
    events_per_batch: { default: 100, min: 1, max: 10000 },
    interval_seconds: { default: 5, min: 1, max: 300 },
    total_duration_seconds: { default: 60, min: 1, max: 3600 },
  });
  useEffect(() => {
    api.get<StreamLimits>("/generate/demo-data/streaming/limits")
      .then((limits) => {
        setStreamLimits(limits);
        // Re-seed the inputs if the user hasn't touched them yet —
        // detect "untouched" by comparing against the prior fallback
        // defaults (100/5/60). If the user already typed a custom
        // value we leave it alone.
        setStreamEventsPerBatch((v) => (v === 100 ? limits.events_per_batch.default : v));
        setStreamIntervalSeconds((v) => (v === 5 ? limits.interval_seconds.default : v));
        setStreamDurationSeconds((v) => (v === 60 ? limits.total_duration_seconds.default : v));
      })
      .catch(() => {
        /* keep fallback bounds — endpoint may be on an older API */
      });
  }, []);
  // Destination mode for streaming events:
  //   "volume"        — JSON files only, no Bronze
  //   "volume_bronze" — files + auto-create Bronze STREAMING TABLE (default)
  //   "direct_table"  — INSERT INTO Delta table directly (no Volume)
  const [streamDestination, setStreamDestination] = useState<"volume" | "volume_bronze" | "direct_table" | "zerobus">("volume_bronze");
  // Whether the Zerobus runtime destination is usable. Fetched once at
  // page load from /demo-data/zerobus/availability — the radio renders
  // disabled when `available === false` so the user sees the option exists
  // but understands they need to fall back to the Phase 1 snippet panel.
  const [zerobusAvailable, setZerobusAvailable] = useState<{ available: boolean; reason: string | null } | null>(null);
  useEffect(() => {
    api.get<{ available: boolean; reason: string | null }>("/generate/demo-data/zerobus/availability")
      .then(setZerobusAvailable)
      .catch(() => setZerobusAvailable({ available: false, reason: "availability check failed" }));
  }, []);

  // ── Performance presets (Streaming Events tab) ──────────────────
  // applyStreamingPreset() bundles destination + cadence into a
  // one-click setup. Named distinctly from the batch-tab applyPreset()
  // above so the two never collide. Values are clamped to streamLimits
  // so an admin-narrowed cap (set via Settings → Performance → Streaming
  // Form Limits) still wins — a toast warns when clamping changes a
  // preset value.
  const applyStreamingPreset = (presetId: StreamPresetId) => {
    if (presetId === "custom") return;
    const preset = STREAMING_PRESETS.find((p) => p.id === presetId);
    if (!preset) return;
    if (preset.requiresZerobus && !zerobusAvailable?.available) {
      toast.error(
        `Zerobus is not available: ${zerobusAvailable?.reason ?? "checking…"}`,
      );
      return;
    }
    const epbClamped = Math.max(
      streamLimits.events_per_batch.min,
      Math.min(streamLimits.events_per_batch.max, preset.events_per_batch),
    );
    const intClamped = Math.max(
      Math.max(1, Math.ceil(streamLimits.interval_seconds.min)),
      Math.min(streamLimits.interval_seconds.max, preset.interval_seconds),
    );
    const durClamped = Math.max(
      streamLimits.total_duration_seconds.min,
      Math.min(streamLimits.total_duration_seconds.max, preset.total_duration_seconds),
    );
    setStreamDestination(preset.destination);
    setStreamEventsPerBatch(epbClamped);
    setStreamIntervalSeconds(intClamped);
    setStreamDurationSeconds(durClamped);
    const clamped = (
      epbClamped !== preset.events_per_batch ||
      intClamped !== preset.interval_seconds ||
      durClamped !== preset.total_duration_seconds
    );
    if (clamped) {
      toast.warning(
        `Preset applied with clamping — your admin has narrower limits than ${preset.label} requires. Edit them in Settings → Performance.`,
      );
    } else {
      toast.success(`Preset applied: ${preset.label}`);
    }
  };

  // Derive which preset (if any) matches the current state. When the
  // user manually edits a field after applying a preset, this drops to
  // "custom" and the preset row's highlight clears — purely visual.
  const activePreset: StreamPresetId = (() => {
    const match = STREAMING_PRESETS.find(
      (p) =>
        p.destination === streamDestination &&
        p.events_per_batch === streamEventsPerBatch &&
        p.interval_seconds === streamIntervalSeconds &&
        p.total_duration_seconds === streamDurationSeconds,
    );
    return match ? match.id : "custom";
  })();
  // Per-request Zerobus credentials. Required only when the user picks
  // destination="zerobus"; the form gates Start/Schedule on these being
  // present (matches the Pydantic validator on the request model).
  //
  // Two auth modes — "oauth" uses an SP's client_id/client_secret (the
  // original path; works on Premium/Enterprise tiers). "pat" reuses the
  // PAT the user logged into the app with — no SP needed, but the
  // Zerobus server may still reject PATs that lack the right scopes.
  // Default "oauth" preserves the existing flow for users who already
  // have a working SP.
  const [zerobusAuthMode, setZerobusAuthMode] = useState<"oauth" | "pat">("oauth");
  const [zerobusServerEndpoint, setZerobusServerEndpoint] = useState("");
  const [zerobusClientId, setZerobusClientId] = useState("");
  const [zerobusClientSecret, setZerobusClientSecret] = useState("");
  // Optional MANAGED LOCATION for new catalogs. Required only when the
  // workspace metastore has no default storage root — without it the
  // CREATE CATALOG IF NOT EXISTS fails with INVALID_STATE. Cloud-
  // agnostic: s3://, abfss://, gs:// all work as long as a UC external
  // location / storage credential covers the path. Leave blank when
  // the catalog already exists or the metastore has a default root.
  const [zerobusCatalogLocation, setZerobusCatalogLocation] = useState("");
  // Helper: paste a Databricks workspace URL → server-side resolver
  // parses it, DNS-probes the AWS region, and auto-fills the Server
  // endpoint field. Browsers can't do DNS, so we delegate.
  const [zerobusDeriveUrl, setZerobusDeriveUrl] = useState("");
  const [zerobusDeriving, setZerobusDeriving] = useState(false);
  const [zerobusDeriveError, setZerobusDeriveError] = useState<string | null>(null);
  // Verify-credentials helper — runs the OAuth client_credentials
  // exchange against the workspace's /oidc/v1/token endpoint, same
  // call the SDK does internally. Decouples credential debugging from
  // a full streaming run (no need to read job logs to know if creds
  // are bad).
  const [zerobusVerifying, setZerobusVerifying] = useState(false);
  const [zerobusVerifyResult, setZerobusVerifyResult] = useState<
    null | { ok: boolean; status_code: number | null; error: string | null; hint: string | null }
  >(null);
  async function verifyZerobusCredentials() {
    if (!zerobusDeriveUrl.trim()) {
      setZerobusVerifyResult({
        ok: false, status_code: null,
        error: "Paste your workspace URL in the field above first — the verifier needs it to know which OAuth endpoint to hit.",
        hint: null,
      });
      return;
    }
    if (!zerobusClientId.trim() || !zerobusClientSecret.trim()) {
      setZerobusVerifyResult({
        ok: false, status_code: null,
        error: "Fill in Client ID and Client secret first.",
        hint: null,
      });
      return;
    }
    setZerobusVerifying(true);
    setZerobusVerifyResult(null);
    try {
      // The endpoint only needs the workspace ROOT URL, not the path/query.
      // Strip whatever the user pasted down to scheme+host.
      let root = zerobusDeriveUrl.trim();
      try {
        const u = new URL(root.startsWith("http") ? root : `https://${root}`);
        root = `${u.protocol}//${u.host}`;
      } catch { /* leave as-is, backend will still try */ }

      const r = await api.post<{ ok: boolean; status_code: number | null; error: string | null; hint: string | null }>(
        "/generate/demo-data/zerobus/verify-credentials",
        {
          workspace_url: root,
          client_id: zerobusClientId.trim(),
          client_secret: zerobusClientSecret.trim(),
        },
      );
      setZerobusVerifyResult(r);
      if (r.ok) toast.success("Zerobus credentials are valid");
    } catch (e: any) {
      setZerobusVerifyResult({
        ok: false, status_code: null,
        error: e?.message ?? "Verify request failed",
        hint: null,
      });
    } finally {
      setZerobusVerifying(false);
    }
  }

  async function deriveZerobusEndpoint() {
    const url = zerobusDeriveUrl.trim();
    if (!url) {
      setZerobusDeriveError("Paste a workspace URL first");
      return;
    }
    setZerobusDeriving(true);
    setZerobusDeriveError(null);
    try {
      const r = await api.post<{
        server_endpoint: string | null;
        workspace_id: string | null;
        region: string | null;
        cloud: string;
        error: string | null;
        region_supported: boolean | null;
        region_single_az: boolean;
      }>("/generate/demo-data/zerobus/derive-endpoint", { workspace_url: url });
      if (r.server_endpoint) {
        setZerobusServerEndpoint(r.server_endpoint);
        // Surface region availability + single-AZ hints inline. We
        // don't block — Databricks publishes region availability
        // separately from connectivity, so a "not on the list" region
        // might still work if the user has early access. Just warn.
        if (r.region_supported === false) {
          setZerobusDeriveError(
            `⚠ Region "${r.region}" is not on the published Zerobus availability list for ${r.cloud.toUpperCase()}. ` +
            `The endpoint was constructed using the standard pattern, but the connection may fail. ` +
            `Verify availability with Databricks support before relying on this in production.`,
          );
          toast.warning(`Resolved ${r.server_endpoint} — region may not be supported`);
        } else if (r.region_single_az) {
          setZerobusDeriveError(
            `Note: region ${r.region} is documented as single-AZ on Azure. The connector works there, ` +
            `but availability characteristics differ from multi-AZ regions.`,
          );
          toast.success(`Resolved ${r.server_endpoint} (single-AZ region)`);
        } else {
          setZerobusDeriveError(null);
          toast.success(`Resolved (${r.cloud} ${r.region}): ${r.server_endpoint}`);
        }
      } else {
        setZerobusDeriveError(r.error ?? "Could not derive endpoint");
      }
    } catch (e: any) {
      setZerobusDeriveError(e?.message ?? "Derive request failed");
    } finally {
      setZerobusDeriving(false);
    }
  }
  const [streamBronzeTable, setStreamBronzeTable] = useState("");
  // Legacy auto-create flag — derived from destination on submit. Kept
  // as state only to render the refresh-cadence input in volume_bronze mode.
  const [streamBronzeRefreshMinutes, setStreamBronzeRefreshMinutes] = useState(5);
  // Streaming job — durable. progressHistory replaces the previous local
  // streamingSeries useState; the hook captures one snapshot per server tick
  // and persists them so the throughput chart restores after navigation.
  const streamJob = useDurableJob({
    key: "demo-data-streaming",
    pollUrl: (id) => `/clone/${id}`,
    pollInterval: 2000,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
    captureProgress: (d) => {
      const prog = d?.progress;
      if (!prog || typeof prog.ticks !== "number" || typeof prog.events_emitted !== "number") return null;
      return {
        tick: prog.ticks,
        elapsed: typeof prog.elapsed_seconds === "number" ? prog.elapsed_seconds : 0,
        events: prog.events_emitted,
        // tickErrors is captured per-snapshot so the chart can mark the
        // exact tick where a failure happened (red dot on the cumulative
        // line). delta + hasError are filled in on read — see
        // streamingSeries memo below.
        tickErrors: typeof prog.tick_errors === "number" ? prog.tick_errors : 0,
      };
    },
    isProgressEqual: (a, b) => a?.tick === b?.tick,
    historyCap: 600,
    notificationTitle: "Streaming complete",
    onComplete: (d) => {
      if (d.status === "completed") toast.success("Streaming emission completed");
      else toast.error(d.error || "Streaming emission failed");
    },
  });
  const streamingJobId = streamJob.jobId;
  const streamingJob = streamJob.entry?.data ?? null;
  // Recompute deltas from the persisted history. The hook stores absolute
  // counts; the chart wants per-tick deltas, so we derive them here.
  // Also computes hasError: true whenever the cumulative tick_errors
  // count went up between two snapshots — used to drop a red dot on
  // that exact tick in the chart.
  const streamingSeries = (() => {
    const hist = streamJob.progressHistory ?? [];
    let lastEvents = 0;
    let lastErrors = 0;
    return hist.map((p: any) => {
      const delta = (p?.events ?? 0) - lastEvents;
      const errorDelta = (p?.tickErrors ?? 0) - lastErrors;
      lastEvents = p?.events ?? lastEvents;
      lastErrors = p?.tickErrors ?? lastErrors;
      return { ...p, delta, hasError: errorDelta > 0 };
    });
  })();
  const streamingEmit = useStreamingEmit();
  const streamingStop = useStreamingStop();

  const toggleIndustry = (industry: string) => {
    setSelectedIndustries((prev) =>
      prev.includes(industry)
        ? prev.filter((i) => i !== industry)
        : [...prev, industry]
    );
  };

  const handleSubmit = async () => {
    if (!catalogName.trim()) {
      toast.error("Catalog name is required");
      return;
    }
    if (selectedIndustries.length === 0) {
      toast.error("Select at least one industry");
      return;
    }

    setSubmitting(true);
    batchJob.clear();

    try {
      const body: any = {
        catalog_name: catalogName.trim(),
        industries: selectedIndustries,
        scale_factor: parseFloat(scaleFactor),
        drop_existing: dropExisting,
        medallion,
        uc_best_practices: ucBestPractices,
        create_functions: createFunctions,
        create_volumes: createVolumes,
        schema_only: schemaOnly,
        realistic_data: realisticData,
        locale,
      };
      // Seed is optional — only send when the user typed a number, otherwise
      // omit so the backend gets None and the Faker output is non-deterministic.
      const seedNum = seed.trim() ? Number.parseInt(seed.trim(), 10) : Number.NaN;
      if (!Number.isNaN(seedNum)) body.seed = seedNum;
      // Theme 2 — DQ profile + anomaly rate. Always sent so the backend
      // can validate them; defaults match server-side defaults.
      body.dq_profile = dqProfile;
      body.anomaly_rate = anomalyRate;
      body.inject_anomalies = injectAnomalies;
      body.data_model = dataModel;
      if (owner.trim()) body.owner = owner.trim();
      if (storageLocation.trim()) body.storage_location = storageLocation.trim();
      if (startDate) body.start_date = startDate;
      if (endDate) body.end_date = endDate;
      if (destCatalog.trim()) body.dest_catalog = destCatalog.trim();

      await batchJob.start(body, async () => {
        const res = await api.post("/generate/demo-data", body);
        if (!res.job_id) throw new Error("Unexpected response — no job_id returned");
        toast.success(`Demo data generation submitted (Job ${res.job_id})`);
        return res.job_id;
      });
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  };

  // Polling for both batch and streaming jobs is handled by useDurableJob —
  // it lives in JobContext so it survives navigation.

  const handleStartStreaming = async () => {
    if (!streamCatalog.trim() || !streamSchema.trim()) {
      toast.error("Catalog and schema are required");
      return;
    }
    if (streamDestination === "zerobus") {
      const missing: string[] = [];
      if (!zerobusServerEndpoint.trim()) missing.push("server endpoint");
      // SP creds are only required in OAuth mode. PAT mode pulls the
      // bearer token from the logged-in client at runtime, so the
      // form doesn't need to collect anything beyond the endpoint.
      if (zerobusAuthMode === "oauth") {
        if (!zerobusClientId.trim()) missing.push("client ID");
        if (!zerobusClientSecret.trim()) missing.push("client secret");
      }
      if (missing.length) {
        toast.error(`Zerobus requires: ${missing.join(", ")}`);
        return;
      }
    }
    try {
      // Wipe previous streaming-job state (including chart history) so a new
      // run starts clean visually instead of merging with the prior series.
      streamJob.clear();
      const params = {
        catalog: streamCatalog.trim(),
        schema: streamSchema.trim(),
        volume: streamVolume.trim() || "events_volume",
        profile: streamProfile,
        events_per_batch: streamEventsPerBatch,
        interval_seconds: streamIntervalSeconds,
        total_duration_seconds: streamDurationSeconds,
        destination: streamDestination,
        bronze_table: streamBronzeTable.trim(),
        auto_create_bronze: streamDestination === "volume_bronze",
        bronze_refresh_minutes: streamBronzeRefreshMinutes,
        // Only thread Zerobus creds when it's actually selected. Sending
        // them on every payload would log secrets unnecessarily and the
        // backend ignores them when destination !== "zerobus". The
        // SP fields are only sent in oauth mode — the API validator
        // doesn't accept them as required when auth_mode='pat', and we
        // don't want stale form state to confuse the request shape.
        ...(streamDestination === "zerobus" && {
          zerobus_server_endpoint: zerobusServerEndpoint.trim(),
          zerobus_auth_mode: zerobusAuthMode,
          ...(zerobusAuthMode === "oauth" && {
            zerobus_client_id: zerobusClientId.trim(),
            zerobus_client_secret: zerobusClientSecret.trim(),
          }),
          // Only send when populated — empty string = "let UC pick the
          // default storage root". The runner treats blank as omitted.
          ...(zerobusCatalogLocation.trim() && {
            zerobus_catalog_location: zerobusCatalogLocation.trim(),
          }),
        }),
      };
      await streamJob.start(params, async () => {
        const res = await streamingEmit.mutateAsync(params);
        if (res?.job_id) toast.success(`Streaming emission started (Job ${res.job_id})`);
        return res?.job_id;
      });
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const handleStopStreaming = async () => {
    if (!streamingJobId) return;
    try {
      await streamingStop.mutateAsync({ job_id: streamingJobId });
      toast.success("Stop requested — runner will halt at next tick");
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  // Built once and shown verbatim in the Auto Loader panel below.
  // Same shape as the SQL the auto-create path executes — keeping
  // the two in sync makes manual re-runs predictable.
  const autoLoaderSnippet = (
    `CREATE OR REFRESH STREAMING TABLE \`${streamCatalog || "<catalog>"}\`.\`${streamSchema || "<schema>"}\`.\`bronze_${streamProfile}\`\n` +
    `SCHEDULE REFRESH CRON '0 0/${streamBronzeRefreshMinutes} * * * ?' AT TIME ZONE 'UTC'\n` +
    `AS SELECT * FROM STREAM read_files(\n` +
    `  '/Volumes/${streamCatalog || "<catalog>"}/${streamSchema || "<schema>"}/${streamVolume || "events_volume"}/${streamProfile}/',\n` +
    `  format => 'json'\n` +
    `);`
  );

  // Auto-scroll logs
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [job?.logs]);

  const handleReset = () => {
    batchJob.clear();
  };

  const handleCleanup = async () => {
    if (!catalogName.trim()) return;
    const confirmed = window.confirm(
      `Are you sure you want to delete the catalog "${catalogName.trim()}"? This action cannot be undone.`
    );
    if (!confirmed) return;

    setCleanupLoading(true);
    try {
      await api.delete(`/generate/demo-data/${catalogName.trim()}`);
      toast.success(`Catalog "${catalogName.trim()}" deleted successfully`);
    } catch (e) {
      toast.error((e as Error).message || "Failed to delete catalog");
    } finally {
      setCleanupLoading(false);
    }
  };

  const toggleIndustryDetail = (industry: string) => {
    setExpandedIndustries((prev) => {
      const next = new Set(prev);
      if (next.has(industry)) next.delete(industry);
      else next.add(industry);
      return next;
    });
  };

  const isRunning = job?.status === "running" || job?.status === "queued";
  const isComplete = job?.status === "completed";
  const isFailed = job?.status === "failed";

  const logs: string[] = job?.logs || job?.log || [];

  // Auto-scroll to bottom of logs when new lines arrive
  useEffect(() => {
    if (isRunning && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs.length, isRunning]);
  const result = job?.result || job?.data || {};

  const formatDuration = (seconds: number) => {
    if (!seconds) return "—";
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return `${m}m ${s}s`;
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Demo Data Generator"
        icon={Database}
        breadcrumbs={["Operations", "Demo Data"]}
        description="Generate realistic demo catalogs with synthetic data across 5 industries — healthcare, financial, retail, telecom, and manufacturing."
      />

      {/* Generator tabs — Batch (one-shot synthetic catalog) and
          Streaming (continuous IoT events to a Volume) are two distinct
          workflows; tabs keep each surface focused. Selection persists
          via sessionStorage so refresh keeps the user where they were. */}
      <div className="flex items-center gap-1 border-b border-border overflow-x-auto">
        {[
          { key: "batch", label: "Batch Catalog", icon: Database, hint: "Generate one-shot synthetic data across N industries" },
          { key: "streaming", label: "Streaming Events", icon: Radio, hint: "Continuously emit IoT events to a UC Volume" },
          { key: "documents", label: "Documents", icon: FileText, hint: "Generate unstructured corpora — PDFs, Office docs, Excel, .eml — for RAG / GenAI demos" },
          { key: "media", label: "Media", icon: ImageIcon, hint: "Generate synthetic images, audio, and video for multimodal AI / vision / speech demos" },
          { key: "knowledge", label: "Knowledge Base", icon: BookOpen, hint: "Generate wiki articles, Q&A pairs, and chat threads for KB-RAG / conversational AI demos" },
          { key: "logs", label: "Logs", icon: ScrollText, hint: "Generate NGINX / JSON / syslog / OTel trace logs for observability and SIEM demos" },
          { key: "code", label: "Code", icon: Code2, hint: "Generate synthetic Python / JS / Java repos for code-search / Copilot-style demos" },
          { key: "capture", label: "Live Capture", icon: Camera, hint: "Capture photos and video from your webcam directly into a UC Volume + Delta table with inline BINARY" },
          { key: "manage", label: "Manage Catalogs", icon: Trash2, hint: "List and drop existing demo catalogs" },
        ].map(({ key, label, icon: TabIcon, hint }) => (
          <button key={key}
            onClick={() => {
              setActiveGenTab(key as typeof activeGenTab);
              try { sessionStorage.setItem("clxs-demo-gen-tab", key); } catch {}
            }}
            title={hint}
            className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px shrink-0 flex items-center gap-1.5 ${
              activeGenTab === key
                ? "border-[#E8453C] text-[#E8453C]"
                : "border-transparent text-muted-foreground hover:text-foreground"
            }`}>
            <TabIcon className="h-3.5 w-3.5" />
            {label}
          </button>
        ))}
      </div>

      {activeGenTab === "batch" && (<>

      {/* Template Presets */}
      <div className="flex flex-wrap items-center gap-3">
        <span className="text-sm font-medium text-muted-foreground">Presets:</span>
        <Button
          variant="outline"
          size="sm"
          onClick={() => applyPreset("quick")}
          disabled={isRunning}
          className="gap-2"
        >
          <Zap className="h-3.5 w-3.5 text-muted-foreground" />
          Quick Demo
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => applyPreset("sales")}
          disabled={isRunning}
          className="gap-2"
        >
          <Zap className="h-3.5 w-3.5 text-[#E8453C]" />
          Sales Demo
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={() => applyPreset("full")}
          disabled={isRunning}
          className="gap-2"
        >
          <Zap className="h-3.5 w-3.5 text-red-500" />
          Full Demo
        </Button>
      </div>

      {/* Configuration Card */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Configuration
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="basics">
            <TabsList className="grid w-full grid-cols-4 h-9">
              <TabsTrigger value="basics">Basics</TabsTrigger>
              <TabsTrigger value="catalog">Catalog Options</TabsTrigger>
              <TabsTrigger value="quality">Data Quality &amp; ML</TabsTrigger>
              <TabsTrigger value="architecture">Architecture</TabsTrigger>
            </TabsList>

            <TabsContent value="basics" className="space-y-5 mt-4">
          {/* Catalog Name */}
          <div className="space-y-1.5">
            <FieldLabel hint="Name of the new demo catalog. Must not already exist unless 'Drop Existing' is checked.">
              Catalog Name <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              value={catalogName}
              onChange={(e) => setCatalogName(e.target.value)}
              placeholder="demo_catalog"
              className="max-w-md"
              disabled={isRunning}
            />
          </div>

          {/* Industries */}
          <div className="space-y-2">
            <FieldLabel hint="Each industry generates a domain-specific schema (e.g. healthcare gets patients, encounters, claims). Pick one for a quick demo, several for cross-domain analytics scenarios.">Industries</FieldLabel>
            <div className="flex flex-wrap gap-3">
              {INDUSTRIES.map((industry) => (
                <label
                  key={industry}
                  className="flex items-center gap-2 text-sm cursor-pointer select-none"
                >
                  <input
                    type="checkbox"
                    checked={selectedIndustries.includes(industry)}
                    onChange={() => toggleIndustry(industry)}
                    disabled={isRunning}
                    className="h-4 w-4 rounded border-gray-300 text-red-600 focus:ring-red-500"
                  />
                  <span className="text-sm capitalize">{industry}</span>
                </label>
              ))}
            </div>
          </div>

          {/* Scale Factor */}
          <div className="space-y-1.5">
            <FieldLabel hint="Multiplier on row counts. 0.01 = ~10M rows total (good for laptop demos); 1.0 = ~1B rows (production-scale benchmark).">Scale Factor</FieldLabel>
            <select
              value={scaleFactor}
              onChange={(e) => setScaleFactor(e.target.value)}
              disabled={isRunning}
              className="flex h-9 w-full max-w-md rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              {SCALE_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {/* Date Range */}
          <div className="grid grid-cols-2 gap-4 max-w-xl">
            <div className="space-y-1.5">
              <FieldLabel hint="Earliest date for generated transactional data (orders, claims, events).">Start Date</FieldLabel>
              <Input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                disabled={isRunning}
              />
            </div>
            <div className="space-y-1.5">
              <FieldLabel hint="Latest date for generated transactional data. Window between start and end determines volume per day.">End Date</FieldLabel>
              <Input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                disabled={isRunning}
              />
            </div>
          </div>
            </TabsContent>

            <TabsContent value="catalog" className="space-y-5 mt-4">
          {/* Owner */}
          <div className="space-y-1.5">
            <FieldLabel hint="Sets the catalog owner principal — usually a team email or group SCIM name. Defaults to the current user.">Owner</FieldLabel>
            <Input
              value={owner}
              onChange={(e) => setOwner(e.target.value)}
              placeholder="team-name or user@domain.com"
              className="max-w-md"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground">Optional. Sets the catalog owner.</p>
          </div>

          {/* Storage Location */}
          <div className="space-y-1.5">
            <FieldLabel hint="External storage URI for managed tables. Required if the workspace doesn't have a default Unity Catalog storage root configured.">Storage Location</FieldLabel>
            <Input
              value={storageLocation}
              onChange={(e) => setStorageLocation(e.target.value)}
              placeholder="abfss://container@storage.dfs.core.windows.net/path"
              className="max-w-xl"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground">Optional. Custom managed storage location for the catalog.</p>
          </div>

          {/* Destination Catalog */}
          <div className="space-y-1.5">
            <FieldLabel hint="If set, the generated catalog is auto-cloned to this destination after generation completes.">Destination Catalog</FieldLabel>
            <Input
              value={destCatalog}
              onChange={(e) => setDestCatalog(e.target.value)}
              placeholder="e.g. prod_catalog"
              className="max-w-xl"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground">Optional. When filled, the generated catalog will be automatically cloned to this destination.</p>
          </div>

          {/* Drop Existing */}
          <div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={dropExisting}
                onChange={(e) => setDropExisting(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-red-600 focus:ring-red-500"
              />
              <span className="text-sm font-medium">Drop Existing</span>
              <InfoDot hint="Drop the catalog if it already exists, then recreate. Without this, generation aborts on conflict." />
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              If checked, the existing catalog will be dropped and recreated.
            </p>
          </div>

          {/* Schema-only — DDL without INSERTs. Cuts generation from
              minutes/hours to seconds for DDL-template verification. */}
          <div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={schemaOnly}
                onChange={(e) => setSchemaOnly(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Schema only (DDL, skip data INSERTs)</span>
              <InfoDot hint="Create catalog, schemas, tables, views, UDFs and volumes — but skip every INSERT. Generation completes in seconds. Useful for verifying DDL templates and CI smoke runs." />
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Skips data generation entirely. The result has 0 rows but every table / view / UDF DDL is created.
            </p>
          </div>
            </TabsContent>

            <TabsContent value="quality" className="space-y-5 mt-4">
          {/* Theme 2 — DQ profile + ML anomaly labels.
              dq_profile picks a named bundle of null/dup/outlier rates;
              anomaly_rate drives the positive-class rate on labeled
              training columns (is_fraud / churn_risk / is_anomaly). */}
          <div className="space-y-2">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <FieldLabel hint="Named bundle of data quality noise. 'clean' injects nothing (perfect for tutorials/screenshots). 'realistic' (default) is small-but-noticeable. 'dirty' makes DQ dashboards meaningful.">
                <span className="text-sm font-medium">DQ profile</span>
                <select
                  className="mt-1 h-9 w-full px-2 text-sm bg-background border border-input rounded-md"
                  value={dqProfile}
                  onChange={(e) => setDqProfile(e.target.value as "clean" | "realistic" | "dirty")}
                  disabled={isRunning}
                >
                  <option value="clean">clean — no DQ noise</option>
                  <option value="realistic">realistic — 5% null, 1% dup (default)</option>
                  <option value="dirty">dirty — 15% null, 5% dup (exercises DQ tools)</option>
                </select>
              </FieldLabel>
              <FieldLabel hint="Positive-class rate for labeled training columns added to fact tables. 0.02 = 2% (typical for unbalanced ML demos). Set to 0 to disable; or untick the checkbox below.">
                <span className="text-sm font-medium">Anomaly rate</span>
                <Input
                  type="number"
                  step={0.01}
                  min={0}
                  max={1}
                  value={anomalyRate}
                  onChange={(e) => {
                    const v = Number.parseFloat(e.target.value);
                    if (!Number.isNaN(v)) setAnomalyRate(Math.max(0, Math.min(1, v)));
                  }}
                  disabled={isRunning || !injectAnomalies}
                  className="mt-1 h-9 text-sm"
                />
              </FieldLabel>
            </div>
            <label className="flex items-center gap-2 text-xs cursor-pointer select-none ml-1">
              <input
                type="checkbox"
                checked={injectAnomalies}
                onChange={(e) => setInjectAnomalies(e.target.checked)}
                disabled={isRunning}
                className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span>Add labeled training columns (<code>is_fraud</code> on financial.transactions, <code>churn_risk</code> on telecom.subscribers, <code>is_anomaly</code> on healthcare.encounters &amp; manufacturing.sensor_readings)</span>
            </label>
          </div>
            </TabsContent>

            <TabsContent value="architecture" className="space-y-5 mt-4">
          {/* Theme: data modeling pattern overlay. "flat" preserves
              today's behaviour. "star_schema" generates additional
              `<industry>_star` schemas with fct_/dim_ tables (DBT-style
              naming) layered on top of the flat data. Future: Data
              Vault 2.0, One Big Table, Snowflake. */}
          <div>
            <FieldLabel hint="How the generated data is laid out. 'Flat' = the existing per-industry schema. 'Star Schema' = adds `<industry>_star` schemas with fact/dim tables (DBT-style fct_*/dim_* naming) materialised via CTAS on top of the flat data.">
              <span className="text-sm font-medium">Data modeling pattern</span>
              <select
                className="mt-1 h-9 w-full px-2 text-sm bg-background border border-input rounded-md"
                value={dataModel}
                onChange={(e) => setDataModel(e.target.value as "flat" | "star_schema")}
                disabled={isRunning}
              >
                <option value="flat">Flat — single per-industry schema (default)</option>
                <option value="star_schema">Star Schema — adds &lt;industry&gt;_star with fct_/dim_ tables</option>
              </select>
            </FieldLabel>
            {dataModel === "star_schema" && (
              <p className="text-xs text-muted-foreground mt-1 ml-1">
                Star Schema overlay materialises a calendar dim, conformed dims with surrogate keys, and fact tables joined to dims. Layered on top of the flat tables (~5% extra time). DBT-style naming: <code>fct_*</code> / <code>dim_*</code>. Skipped on schema-only.
              </p>
            )}
          </div>

          {/* Realistic data (Faker) — when enabled, name/email/phone columns
              sample from locale-aware Faker pools instead of the legacy
              hardcoded "James"/"Mary"/"patient1@example.com" pools. */}
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={realisticData}
                onChange={(e) => setRealisticData(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Realistic data (Faker)</span>
              <InfoDot hint="Replace the small static name / email / phone pools with locale-aware Faker output. Off by default to preserve test fixtures matching legacy values." />
            </label>
            {realisticData && (
              <div className="ml-6 grid grid-cols-1 md:grid-cols-2 gap-3">
                <FieldLabel hint="Faker locale code. en_US for American names, en_GB for British, de_DE for German, etc.">
                  <span className="text-xs">Locale</span>
                  <select
                    className="mt-1 h-8 w-full px-2 text-sm bg-background border border-input rounded-md"
                    value={locale}
                    onChange={(e) => setLocale(e.target.value)}
                    disabled={isRunning}
                  >
                    <option value="en_US">en_US — American English</option>
                    <option value="en_GB">en_GB — British English</option>
                    <option value="de_DE">de_DE — German</option>
                    <option value="fr_FR">fr_FR — French</option>
                    <option value="es_ES">es_ES — Spanish</option>
                    <option value="ja_JP">ja_JP — Japanese</option>
                    <option value="zh_CN">zh_CN — Simplified Chinese</option>
                    <option value="hi_IN">hi_IN — Hindi (India)</option>
                  </select>
                </FieldLabel>
                <FieldLabel hint="Optional integer seed for deterministic Faker output. Same seed → same generated names across runs. Leave blank for non-deterministic.">
                  <span className="text-xs">Seed (optional)</span>
                  <Input
                    type="number"
                    placeholder="e.g. 42"
                    value={seed}
                    onChange={(e) => setSeed(e.target.value)}
                    disabled={isRunning}
                    className="mt-1 h-8 text-sm"
                  />
                </FieldLabel>
              </div>
            )}
          </div>

          {/* Medallion Architecture */}
          <div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={medallion}
                onChange={(e) => setMedallion(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Medallion Architecture (Bronze / Silver / Gold)</span>
              <InfoDot hint="Generate bronze → silver → gold schemas per industry, mirroring the standard Lakehouse layering." />
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates bronze (raw), silver (cleaned), and gold (aggregated) schemas per industry.
            </p>
          </div>

          {/* UC Best Practices */}
          {medallion && (
            <div className="ml-6">
              <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={ucBestPractices}
                  onChange={(e) => setUcBestPractices(e.target.checked)}
                  disabled={isRunning}
                  className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
                />
                <span className="text-sm font-medium">UC Best Practices Naming</span>
                <InfoDot hint="Shared naming (bronze.healthcare_patients) instead of legacy per-industry schemas (healthcare_bronze.patients). Recommended for new deployments." />
              </label>
              <p className="text-xs text-muted-foreground mt-1 ml-6">
                {ucBestPractices
                  ? "Shared schemas: bronze, silver, gold — tables prefixed with industry name (e.g. bronze.healthcare_raw_claims)"
                  : "Legacy naming: healthcare_bronze, healthcare_silver, healthcare_gold — separate schema per industry"
                }
              </p>
              <a
                href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-[#E8453C] hover:underline ml-6 mt-0.5 inline-block"
              >
                Unity Catalog best practices
              </a>
            </div>
          )}

          {/* Create Functions */}
          <div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={createFunctions}
                onChange={(e) => setCreateFunctions(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Create UDFs (User-Defined Functions)</span>
              <InfoDot hint="Generate ~20 SQL UDFs per industry — masking, formatters, validators, business calculations. Adds ~30s to generation." />
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates 20 SQL UDFs per industry for masking, formatting, validation, and business logic.
            </p>
          </div>

          {/* Create Volumes */}
          <div>
            <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
              <input
                type="checkbox"
                checked={createVolumes}
                onChange={(e) => setCreateVolumes(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Create Volumes with Sample Files</span>
              <InfoDot hint="Create one managed volume per industry and seed it with sample CSV exports (1000 rows/table). Useful for ingestion demos." />
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates managed volumes and exports sample CSV files (1000 rows per table).
            </p>
          </div>
            </TabsContent>
          </Tabs>

          {/* Submit / Reset buttons */}
          <div className="flex items-center gap-3 pt-2">
            <Button
              onClick={handleSubmit}
              disabled={!catalogName.trim() || selectedIndustries.length === 0 || submitting || isRunning}
              className="bg-red-600 hover:bg-red-700 text-white"
            >
              {submitting || isRunning ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              {submitting ? "Submitting..." : isRunning ? "Generating..." : "Generate Demo Data"}
            </Button>
            {/* Theme 4 — Export config as JSON. Round-trippable: paste back
                into POST /generate/demo-data to reproduce the exact form
                state. Useful for reusing presets across machines / sharing
                a "Sales Demo Mk II" config in a Slack thread. */}
            <Button
              variant="outline"
              onClick={() => {
                const config = {
                  catalog_name: catalogName,
                  industries: selectedIndustries,
                  scale_factor: Number.parseFloat(scaleFactor),
                  owner: owner || undefined,
                  storage_location: storageLocation || undefined,
                  start_date: startDate,
                  end_date: endDate,
                  dest_catalog: destCatalog || undefined,
                  drop_existing: dropExisting,
                  medallion,
                  uc_best_practices: ucBestPractices,
                  create_functions: createFunctions,
                  create_volumes: createVolumes,
                  schema_only: schemaOnly,
                  realistic_data: realisticData,
                  locale,
                  seed: seed.trim() ? Number.parseInt(seed.trim(), 10) : undefined,
                  dq_profile: dqProfile,
                  anomaly_rate: anomalyRate,
                  inject_anomalies: injectAnomalies,
                };
                const blob = new Blob([JSON.stringify(config, null, 2)], { type: "application/json" });
                const a = document.createElement("a");
                a.href = URL.createObjectURL(blob);
                a.download = `${(catalogName || "demo").trim()}-config.json`;
                a.click();
                URL.revokeObjectURL(a.href);
                toast.success("Config exported");
              }}
              disabled={!catalogName.trim() || selectedIndustries.length === 0}
            >
              <Download className="h-4 w-4 mr-2" />
              Export JSON
            </Button>
            {(isComplete || isFailed) && (
              <Button variant="outline" onClick={handleReset}>
                <RefreshCw className="h-4 w-4 mr-2" />
                Reset
              </Button>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Preview & Estimated Cost Section */}
      {selectedIndustries.length > 0 && (
        <Card className="border-border dark:border-border bg-muted/30 dark:bg-white/5">
          <CardHeader className="pb-2">
            <button
              onClick={() => setPreviewOpen((o) => !o)}
              className="flex items-center justify-between w-full text-left"
            >
              <CardTitle className="flex items-center gap-2 text-base">
                <Info className="h-4 w-4 text-[#E8453C]" />
                Generation Preview
              </CardTitle>
              {previewOpen ? (
                <ChevronUp className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              )}
            </button>
          </CardHeader>
          {previewOpen && (
            <CardContent className="space-y-4 pt-0">
              {/* Stats Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{schemasCount}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Schemas</p>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{estimatedTables}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Est. Tables</p>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{estimatedViews}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Est. Views</p>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{estimatedUdfs}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Est. UDFs</p>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{formatNumber(estimatedRows)}</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Est. Rows</p>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center shadow-sm">
                  <p className="text-xl font-bold text-[#E8453C] dark:text-[#E8453C]">{estimatedStorageGb.toFixed(2)} GB</p>
                  <p className="text-xs text-muted-foreground mt-0.5">Est. Storage</p>
                </div>
              </div>

              {/* Estimated Costs */}
              <div className="flex flex-wrap gap-4 pt-1">
                <div className="flex items-center gap-2 bg-white dark:bg-gray-900 rounded-lg px-4 py-2.5 shadow-sm">
                  <span className="text-foreground font-bold text-sm">{currSymbol}</span>
                  <div>
                    <p className="text-sm font-semibold text-foreground">
                      {currSymbol}{estimatedStorageCost.toFixed(4)}
                      <span className="text-xs font-normal text-muted-foreground"> /month</span>
                    </p>
                    <p className="text-xs text-muted-foreground">Est. Storage Cost (@ {currSymbol}{storagePricePerGb}/GB)</p>
                  </div>
                </div>
                <div className="flex items-center gap-2 bg-white dark:bg-gray-900 rounded-lg px-4 py-2.5 shadow-sm">
                  <Zap className="h-4 w-4 text-muted-foreground" />
                  <div>
                    <p className="text-sm font-semibold text-foreground">
                      {estimatedDbus.toFixed(1)} DBUs
                    </p>
                    <p className="text-xs text-muted-foreground">Est. Compute ({industriesCount} industries x {scale} scale x 50)</p>
                  </div>
                </div>
              </div>

              {/* Theme 4 — Server-computed per-industry breakdown. Toggle via
                  "Get accurate preview" button so we don't hit the endpoint on
                  every keystroke. The numbers here come from the same
                  `preview_demo_catalog` helper the orchestrator uses internally,
                  so they're closer to actual generation reality than the
                  client-side static estimates above. */}
              <div className="border-t pt-3 mt-2">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-medium">Per-industry breakdown</div>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={fetchLivePreview}
                    disabled={livePreviewLoading || isRunning || selectedIndustries.length === 0}
                  >
                    {livePreviewLoading ? "Estimating…" : livePreview ? "Refresh" : "Get accurate preview"}
                  </Button>
                </div>
                {livePreview && (
                  <div className="space-y-2">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs">
                      {livePreview.per_industry.map((p) => (
                        <div key={p.industry} className="flex items-center justify-between bg-white dark:bg-gray-900 rounded px-3 py-2 shadow-sm">
                          <div className="font-medium capitalize">{p.industry.replaceAll("_", " ")}</div>
                          <div className="text-muted-foreground">
                            {p.tables} tables · {formatNumber(p.rows)} rows · {(p.estimated_bytes / (1024 ** 3)).toFixed(2)} GB · ~{p.estimated_duration_seconds.toFixed(0)}s
                          </div>
                        </div>
                      ))}
                    </div>
                    <div className="flex items-center gap-3 text-xs text-muted-foreground pt-1">
                      <span>
                        Total: <strong className="text-foreground">{formatNumber(livePreview.total_rows)}</strong> rows ·
                        <strong className="text-foreground"> {livePreview.total_gb.toFixed(2)} GB</strong> ·
                        ~<strong className="text-foreground">{(livePreview.estimated_duration_seconds / 60).toFixed(1)} min</strong>
                      </span>
                      <span>
                        First-month cost: <strong className="text-foreground">${livePreview.estimated_cost_usd.first_month_total.toFixed(2)}</strong>
                        {" "}({" "}${livePreview.estimated_cost_usd.monthly_storage.toFixed(2)} storage + ${livePreview.estimated_cost_usd.one_time_compute.toFixed(2)} compute)
                      </span>
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          )}
        </Card>
      )}

      {/* Industry Detail Cards */}
      {selectedIndustries.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <button
              onClick={() => setIndustryDetailOpen((o) => !o)}
              className="flex items-center justify-between w-full text-left"
            >
              <CardTitle className="flex items-center gap-2 text-base">
                <Database className="h-4 w-4 text-muted-foreground" />
                Industry Table Details
                <span className="text-xs font-normal text-muted-foreground ml-1">
                  ({selectedIndustries.length} industries, {estimatedTables} tables)
                </span>
              </CardTitle>
              {industryDetailOpen ? (
                <ChevronUp className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              )}
            </button>
          </CardHeader>
          {industryDetailOpen && (
            <CardContent className="space-y-2 pt-0">
              {selectedIndustries.map((industry) => {
                const tables = INDUSTRY_TABLES[industry] || [];
                const isExpanded = expandedIndustries.has(industry);
                return (
                  <div key={industry} className="border rounded-lg overflow-hidden">
                    <button
                      onClick={() => toggleIndustryDetail(industry)}
                      className="flex items-center justify-between w-full px-4 py-2.5 bg-gray-50 dark:bg-gray-900 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors text-left"
                    >
                      <span className="text-sm font-medium capitalize">{industry}</span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-muted-foreground">{tables.length} tables</span>
                        {isExpanded ? (
                          <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" />
                        ) : (
                          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                        )}
                      </div>
                    </button>
                    {isExpanded && (
                      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-px bg-gray-200 dark:bg-gray-700">
                        {tables.map((tbl) => (
                          <div key={tbl.name} className="bg-white dark:bg-gray-950 px-3 py-2 flex items-center justify-between">
                            <span className="text-xs font-mono text-foreground">{tbl.name}</span>
                            <span className="text-xs text-muted-foreground ml-2">{formatNumber(tbl.rows * scale)}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </CardContent>
          )}
        </Card>
      )}

      {/* Progress Section */}
      {jobId && job && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                {isRunning && <Loader2 className="h-5 w-5 text-[#E8453C] animate-spin" />}
                {isComplete && <CheckCircle2 className="h-5 w-5 text-green-500" />}
                {isFailed && <XCircle className="h-5 w-5 text-red-600" />}
                Job Progress
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-muted-foreground font-normal">Job {jobId}</span>
                {statusBadge(job.status)}
                {isRunning && (
                  <Button size="sm" variant="destructive" className="h-7 text-xs"
                    onClick={async () => {
                      try {
                        // Backend cancellation is `DELETE /api/clone/{job_id}`
                        // (see api/routers/clone.py:cancel_job). The previous
                        // `POST /clone/{id}/cancel` call returned 405 because
                        // that path doesn't exist — the closest match is the
                        // GET endpoint at the same prefix, which rejected POST.
                        await api.delete(`/clone/${jobId}`);
                        toast.success("Job cancelled");
                      } catch (e: any) {
                        toast.error("Cancel failed: " + (e.message || ""));
                      }
                    }}>
                    Cancel
                  </Button>
                )}
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Per-industry progress */}
            {job?.progress?.current_industry && (
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">
                    Industry: <span className="capitalize">{job.progress.current_industry}</span>{" "}
                    ({job.progress.industry_index}/{job.progress.total_industries})
                  </span>
                  <Badge className="bg-muted/40 text-foreground dark:bg-gray-800 dark:text-gray-200 text-xs">
                    {job.progress.current_phase}
                  </Badge>
                </div>
                {/* Overall progress bar */}
                <div>
                  <div className="flex items-center justify-between text-xs text-muted-foreground mb-1">
                    <span>Overall progress</span>
                    <span>
                      {Math.min(100, Math.round((job.progress.industry_index / job.progress.total_industries) * 100))}%
                      {(() => {
                        const startedAt = job?.started_at ? new Date(job.started_at).getTime() : Date.now();
                        const elapsed = (Date.now() - startedAt) / 1000;
                        const progress = job?.progress?.industry_index || 0;
                        const total = job?.progress?.total_industries || 1;
                        const eta = progress > 0 ? Math.round((elapsed / progress) * (total - progress)) : 0;
                        if (eta > 0) {
                          const mins = Math.floor(eta / 60);
                          const secs = eta % 60;
                          return ` — ETA: ~${mins}m ${secs}s`;
                        }
                        return "";
                      })()}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                    <div
                      className="bg-[#E8453C] h-2 rounded-full transition-all duration-300"
                      style={{ width: `${Math.min(100, (job.progress.industry_index / job.progress.total_industries) * 100)}%` }}
                    />
                  </div>
                </div>
                {/* Table progress if in tables phase */}
                {job.progress.current_phase === "tables" && job.progress.tables_total > 0 && (
                  <div>
                    <div className="flex items-center justify-between text-xs text-muted-foreground mb-1">
                      <span>Tables: {job.progress.tables_done}/{job.progress.tables_total}</span>
                      <span>{Math.round((job.progress.tables_done / job.progress.tables_total) * 100)}%</span>
                    </div>
                    <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5">
                      <div
                        className="bg-muted/200 h-1.5 rounded-full transition-all duration-300"
                        style={{ width: `${(job.progress.tables_done / job.progress.tables_total) * 100}%` }}
                      />
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Live Logs */}
            {Array.isArray(logs) && logs.length > 0 && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <label className="text-sm font-medium flex items-center gap-2">
                    {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
                    Logs
                    <Badge variant="outline" className="text-[10px] px-1.5">{logs.length} lines</Badge>
                  </label>
                  <div className="flex items-center gap-1">
                    <Button variant="ghost" size="sm" className="h-6 px-2" onClick={async () => {
                      await navigator.clipboard.writeText(logs.join("\n"));
                      toast.success("Logs copied");
                    }} title="Copy logs">
                      <ClipboardCopy className="h-3 w-3 mr-1" />
                      <span className="text-xs">Copy</span>
                    </Button>
                    <Button variant="ghost" size="sm" className="h-6 px-2" onClick={() => {
                      const blob = new Blob([logs.join("\n")], { type: "text/plain" });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement("a");
                      a.href = url;
                      a.download = `demo-data-${jobId || "logs"}.log`;
                      a.click();
                      URL.revokeObjectURL(url);
                    }} title="Download logs">
                      <Download className="h-3 w-3" />
                    </Button>
                  </div>
                </div>
                <div className="bg-[#0d1117] text-gray-300 rounded-lg p-3 overflow-x-auto text-xs font-mono max-h-[400px] overflow-y-auto">
                  {logs.map((line: string, i: number) => (
                    <div key={i} className={`whitespace-pre-wrap leading-relaxed ${
                      // Reserve red shades for actual ERRORs. In-progress
                      // "Creating …" / "Generating …" lines render cyan so
                      // users don't mistake them for failures (the brand
                      // red used elsewhere is the same hue as the ERROR
                      // colour, which was confusing).
                      line.includes("ERROR") ? "text-red-400" :
                      line.includes("WARNING") ? "text-gray-400" :
                      line.includes("done") || line.includes("Created") || line.includes("created") ? "text-gray-300" :
                      line.includes("Creating") || line.includes("Generating") ? "text-cyan-400" : ""
                    }`}>
                      {line}
                    </div>
                  ))}
                  <div ref={logsEndRef} />
                </div>
              </div>
            )}

            {/* Error message */}
            {isFailed && (job.error || job.message) && (
              <div className="flex items-center gap-3 p-3 bg-red-50 dark:bg-red-950/30 rounded-lg">
                <XCircle className="h-5 w-5 text-red-600 shrink-0" />
                <div>
                  <p className="text-sm font-medium text-red-800 dark:text-red-300">Generation failed</p>
                  <p className="text-xs text-red-600 dark:text-red-400">{job.error || job.message}</p>
                </div>
              </div>
            )}

            {/* Post-completion actions */}
            {isComplete && (
              <div className="flex items-center gap-3 pt-2">
                <a
                  href={`/explore?catalog=${encodeURIComponent(catalogName.trim())}`}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-[#E8453C] hover:bg-[#D93025] text-white text-sm font-medium transition-colors"
                >
                  <ExternalLink className="h-4 w-4" />
                  Explore Catalog
                </a>
                <Button
                  variant="outline"
                  className="border-red-300 text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-950/30"
                  onClick={handleCleanup}
                  disabled={cleanupLoading}
                >
                  {cleanupLoading ? (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Trash2 className="h-4 w-4 mr-2" />
                  )}
                  {cleanupLoading ? "Deleting..." : "Cleanup Catalog"}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Summary Card — shown on completion */}
      {isComplete && result && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <CheckCircle2 className="h-5 w-5 text-foreground" />
              Generation Summary
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.schemas_created ?? result.schemas ?? "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Schemas Created</p>
              </div>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.tables_created ?? result.tables ?? "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Tables</p>
              </div>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.views_created ?? result.views ?? "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Views</p>
              </div>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.udfs_created ?? result.udfs ?? "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">UDFs</p>
              </div>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.total_rows != null
                    ? Number(result.total_rows).toLocaleString()
                    : "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Total Rows</p>
              </div>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 text-center">
                <p className="text-2xl font-bold text-foreground">
                  {result.duration_seconds
                    ? formatDuration(result.duration_seconds)
                    : result.duration ?? "—"}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Duration</p>
              </div>
            </div>

            {/* Theme 4 — FK relationship diagram. The orchestrator's
                referential-integrity audit (Theme 3) emits per-FK orphan
                counts; render them as a compact list so users can see at
                a glance which dim/fact joins are clean and which have
                drift. Hidden when the audit was skipped (schema_only) or
                turned off via validate_referential_integrity=False. */}
            {result.referential_integrity?.details && (
              <div className="mt-4 border-t pt-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-medium">Foreign-key integrity audit</div>
                  <span className="text-xs text-muted-foreground">
                    {result.referential_integrity.orphan_free}/{result.referential_integrity.checks_run} FKs orphan-free
                    {result.referential_integrity.with_orphans > 0 && (
                      <span className="ml-2 text-amber-700 dark:text-amber-400">
                        · {result.referential_integrity.with_orphans} with orphans
                      </span>
                    )}
                  </span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-1.5 text-xs">
                  {result.referential_integrity.details.map((d: any, i: number) => {
                    const ok = !d.error && (d.orphans ?? 0) === 0;
                    const skipped = !!d.error;
                    return (
                      <div key={`${d.industry}-${d.child}-${d.fk}-${i}`} className="flex items-center justify-between bg-gray-50 dark:bg-gray-900 rounded px-2 py-1.5">
                        <span className="font-mono text-[11px]">
                          {d.industry}.{d.child}.{d.fk} → {d.parent}.{d.parent_pk}
                        </span>
                        {skipped ? (
                          <span className="text-muted-foreground text-[10px]">skipped</span>
                        ) : ok ? (
                          <span className="text-emerald-600 dark:text-emerald-400 text-[10px]">✓ {d.child_sampled?.toLocaleString()} sampled</span>
                        ) : (
                          <span className="text-amber-600 dark:text-amber-400 text-[10px]" title={d.parent_has_row_filter ? `Row filter on ${d.parent} likely — filtered-but-real rows appear as orphans for non-admins` : undefined}>
                            {d.orphans?.toLocaleString()} orphans ({d.orphan_pct}%)
                            {d.parent_has_row_filter && <span className="ml-1 opacity-70">(row filter)</span>}
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Theme 2 — labeled training columns rollup. Surfaces what
                ML target columns were added and at what positive-class
                rate, so the demo is self-describing. */}
            {result.anomalies && result.anomalies.length > 0 && (
              <div className="mt-4 border-t pt-4">
                <div className="text-sm font-medium mb-2">Labeled training columns</div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-1.5 text-xs">
                  {result.anomalies.map((a: any, i: number) => (
                    <div key={`anom-${a.industry}-${a.table}-${a.column}-${i}`} className="bg-gray-50 dark:bg-gray-900 rounded px-2 py-1.5 font-mono text-[11px]">
                      <span className="text-emerald-600 dark:text-emerald-400">+ </span>
                      {a.industry}.{a.table}.{a.column}
                      <span className="text-muted-foreground"> ({a.sql_type}, ~{(a.anomaly_rate * 100).toFixed(1)}% positive)</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Star Schema modeling layer rollup — per-industry schema +
                fact / dim counts. Hidden when data_model="flat" (no key on
                result), and on per-industry skipped/error entries we
                annotate the row instead of dropping it (signals that the
                user picked an industry without a registry entry). */}
            {result.star_schema?.per_industry && (
              <div className="mt-4 border-t pt-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-medium">Star Schema modeling layer</div>
                  <span className="text-xs text-muted-foreground">
                    {result.star_schema.facts_created} facts + {result.star_schema.dims_created} dims across {result.star_schema.schemas_created?.length ?? 0} schemas
                  </span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-1.5 text-xs">
                  {result.star_schema.per_industry.map((s: any, i: number) => (
                    <div key={`star-${s.industry}-${i}`} className="bg-gray-50 dark:bg-gray-900 rounded px-2 py-1.5 font-mono text-[11px]">
                      {s.error ? (
                        <span className="text-red-500">✗ {s.industry}: {s.error}</span>
                      ) : s.skipped ? (
                        <span className="text-muted-foreground">— {s.industry}: skipped ({s.reason})</span>
                      ) : (
                        <>
                          <span className="text-emerald-600 dark:text-emerald-400">✓ </span>
                          <span className="font-medium">{s.schema}</span>
                          <span className="text-muted-foreground"> · {s.facts_created} facts · {s.dims_created} dims</span>
                        </>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      </>)}

      {activeGenTab === "streaming" && (
      <>
      {/* ─── Streaming emission card ─── */}
      <Card className="bg-card border-border">
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Radio className="h-4 w-4 text-[#E8453C]" />
            Streaming emission (IoT demo)
            <Badge variant="outline" className="text-[10px] ml-1">Beta</Badge>
          </CardTitle>
          <p className="text-xs text-muted-foreground mt-1">
            Continuously emit JSON event batches to a UC Volume — Auto Loader / DLT consumes the files.
            Choose a built-in device profile or auto-create a streaming Bronze Delta table.
          </p>
        </CardHeader>
          <CardContent className="space-y-5">
            {/* Performance presets — one-click bundles of destination + cadence
                tuned for different throughput tiers. Active preset gets a
                primary border; "Custom" auto-lights when fields drift from
                any preset. Zerobus preset disables when the SDK isn't
                available, mirroring the destination radio. */}
            <div className="border border-dashed border-border rounded-md p-3 bg-muted/20">
              <FieldLabel hint="One-click presets that set destination + events-per-batch + interval + duration to combinations tuned for different throughput tiers. Click any preset to apply; manually editing a field below switches the indicator to Custom. Throughput numbers are typical for a small/medium DBSQL Serverless warehouse — your mileage will vary by warehouse size and event-shape complexity.">
                Performance preset
              </FieldLabel>
              <div className="mt-2 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2">
                {STREAMING_PRESETS.map((preset) => {
                  const isActive = activePreset === preset.id;
                  const disabled = !!preset.requiresZerobus && !zerobusAvailable?.available;
                  const disabledReason = preset.requiresZerobus
                    ? (zerobusAvailable?.reason ?? "Checking availability...")
                    : null;
                  return (
                    <button
                      key={preset.id}
                      type="button"
                      onClick={() => applyStreamingPreset(preset.id)}
                      disabled={disabled}
                      title={disabled ? (disabledReason ?? undefined) : undefined}
                      className={`flex flex-col items-start gap-0.5 p-2 border rounded-md text-xs transition-colors text-left ${
                        disabled
                          ? "border-input bg-muted/20 opacity-60 cursor-not-allowed"
                          : isActive
                            ? "border-[#E8453C] bg-[#E8453C]/5 cursor-pointer"
                            : "border-input hover:bg-muted/30 cursor-pointer"
                      }`}
                    >
                      <div className="font-medium">{preset.label}</div>
                      <div className="text-[10px] text-muted-foreground">{preset.desc}</div>
                    </button>
                  );
                })}
              </div>
              {activePreset === "custom" && (
                <p className="text-[10px] text-muted-foreground mt-2">
                  Custom — current settings don&apos;t match any preset. Pick a preset above to reset, or keep editing.
                </p>
              )}
            </div>

            {/* Destination mode — controls which downstream fields are visible
                and what the runner does each tick. */}
            <div className="border border-dashed border-border rounded-md p-3 bg-muted/20">
              <FieldLabel hint="Volume only: emit JSON files; you wire Auto Loader yourself. Volume + Bronze: same files plus an auto-created STREAMING TABLE on a CRON refresh (needs DBSQL Serverless tier that supports it). Direct to table: each tick INSERTs straight into a Delta table — no Volume, no Auto Loader, works on any tier including Free Edition. Zerobus: direct gRPC append via the Databricks Zerobus low-latency API. Requires Premium/Enterprise tier with a UC External Location + the destination schema configured with managed storage — NOT compatible with Free Edition. On Free Edition, use Direct to table instead, or paste the snippet panel below into a Premium workspace.">
                Destination
              </FieldLabel>
              <div className="mt-2 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2">
                {([
                  {
                    val: "volume",
                    title: "Volume only",
                    sub: "JSON files → Volume",
                    disabled: false,
                    disabledReason: null,
                    // Per-destination warehouse impact. tone:
                    //   "none"  — warehouse not used at all (green)
                    //   "low"   — used once at startup for DDL only (green)
                    //   "high"  — used every tick; warehouse size matters (amber)
                    whImpact: { tone: "none", text: "Warehouse: not used. Files write directly to UC Volume." },
                  },
                  {
                    val: "volume_bronze",
                    title: "Volume + Bronze",
                    sub: "Files + Auto Loader STREAMING TABLE",
                    disabled: false,
                    disabledReason: null,
                    whImpact: { tone: "low", text: "Warehouse: one-time CREATE OR REFRESH STREAMING TABLE. Refresh runs on its own DBSQL Serverless pool." },
                  },
                  {
                    val: "direct_table",
                    title: "Direct to table",
                    sub: "INSERT each batch into Delta (no Volume)",
                    disabled: false,
                    disabledReason: null,
                    whImpact: { tone: "high", text: "Warehouse: every tick. INSERT VALUES is single-driver-bound — pick the largest serverless you can." },
                  },
                  {
                    val: "zerobus",
                    title: "Zerobus",
                    sub: "Direct gRPC append (low-latency) — Premium/Enterprise tier",
                    // Disabled until the availability check returns true.
                    // While the check is in flight (zerobusAvailable===null)
                    // we keep the option disabled too — better to render
                    // disabled-then-enabled than enabled-then-disabled.
                    disabled: !zerobusAvailable?.available,
                    disabledReason: zerobusAvailable?.reason ?? "Checking availability...",
                    whImpact: { tone: "low", text: "Warehouse: one-time DDL only (CREATE TABLE + GRANTs). Idle during streaming. Smallest warehouse is fine." },
                  },
                ] as const).map((opt) => (
                  <label
                    key={opt.val}
                    title={opt.disabled ? (opt.disabledReason ?? undefined) : undefined}
                    className={`flex items-start gap-2 p-2 border rounded-md text-xs transition-colors ${
                      opt.disabled
                        ? "border-input bg-muted/20 opacity-60 cursor-not-allowed"
                        : streamDestination === opt.val
                          ? "border-[#E8453C] bg-[#E8453C]/5 cursor-pointer"
                          : "border-input hover:bg-muted/30 cursor-pointer"
                    }`}
                  >
                    <input
                      type="radio"
                      name="stream-destination"
                      value={opt.val}
                      checked={streamDestination === opt.val}
                      disabled={opt.disabled}
                      onChange={() => setStreamDestination(opt.val as typeof streamDestination)}
                      className="mt-0.5 h-3.5 w-3.5 text-[#E8453C] focus:ring-[#E8453C]"
                    />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium flex items-center gap-1">
                        {opt.title}
                        {opt.val === "zerobus" && (
                          <Badge variant="outline" className="text-[9px]">Preview</Badge>
                        )}
                      </div>
                      <div className="text-[10px] text-muted-foreground">{opt.sub}</div>
                      {/* Warehouse-impact line — green for none/low, amber for high.
                          Uses Tailwind's emerald/amber palette which renders sensibly
                          in both light and dark themes. */}
                      <div
                        className={`mt-1 text-[10px] italic leading-snug ${
                          opt.whImpact.tone === "high"
                            ? "text-amber-600 dark:text-amber-400"
                            : "text-emerald-600 dark:text-emerald-400"
                        }`}
                      >
                        {opt.whImpact.text}
                      </div>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {/* Zerobus credentials — visible only when destination=zerobus
                AND the SDK is available. The form's Pydantic validator on
                the backend will 422 if any of these are blank, but we also
                client-side guard handleStartStreaming to fail earlier.
                Step-by-step layout: numbered circles that turn into green
                checks once each step's "done" predicate is satisfied. The
                step numbers are computed (not hard-coded) because step 4
                only exists in OAuth mode — PAT mode skips Verify and the
                storage step renumbers down. */}
            {streamDestination === "zerobus" && zerobusAvailable?.available && (() => {
              // Per-step done predicates. These drive the numbered-circle →
              // green-check transition AND let us decide which steps still
              // hold the user back from clicking Start streaming.
              const step1Done = true; // auth mode always picked (default oauth)
              const step2Done = !!zerobusServerEndpoint.trim();
              const step3Done = zerobusAuthMode === "pat"
                || (!!zerobusClientId.trim() && !!zerobusClientSecret.trim());
              const step4Done = zerobusVerifyResult?.ok === true;
              const showVerifyStep = zerobusAuthMode === "oauth";
              // Step 5 (storage) is optional → never affects the "all done"
              // computation but still gets a number for orientation.
              const storageStepNumber = showVerifyStep ? 5 : 4;
              return (
              <div className="border border-border rounded-md bg-muted/20 p-3 space-y-3">
                <div className="flex items-center gap-2">
                  <Radio className="h-3.5 w-3.5 text-[#E8453C]" />
                  <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                    Zerobus credentials
                  </span>
                  <Badge variant="outline" className="text-[10px]">Required</Badge>
                </div>
                <p className="text-[11px] text-muted-foreground">
                  Zerobus uses a region-specific gRPC endpoint. Two auth modes:
                  pick <strong>OAuth</strong> if you have a service principal already
                  set up (the original Zerobus contract), or <strong>PAT</strong> to
                  reuse the token you logged into this app with — no SP needed.
                  Secrets stay in your browser session and are sent only when
                  starting a Zerobus run.
                </p>

                {/* Prerequisites (one-time admin work outside the form).
                    Collapsed by default — most users only need it once and
                    don't need to re-read it on every visit. Click to expand. */}
                <details className="text-[11px] text-muted-foreground border-l-2 border-amber-300 pl-3 py-1 bg-amber-50/30 rounded-sm">
                  <summary className="cursor-pointer text-amber-700 font-medium">
                    One-time admin prerequisite (Premium/Enterprise tier)
                  </summary>
                  <div className="space-y-1 mt-1.5">
                    <div>
                      Zerobus only writes to managed Delta tables in non-default
                      storage. As a workspace admin, run once on the destination
                      schema:
                    </div>
                    <pre className="text-[10px] bg-background border border-border rounded p-1.5 overflow-x-auto">{`ALTER SCHEMA \`<catalog>\`.\`<schema>\` SET MANAGED LOCATION 's3://your-bucket/path';`}</pre>
                    <div>
                      Without this, the run fails with{" "}
                      <code className="bg-background px-1 rounded">
                        Error Code: 4024 — Unsupported table kind
                      </code>.
                    </div>
                    <div className="border-t border-amber-300/50 pt-1 mt-1">
                      <strong className="text-amber-700">On Databricks Free Edition?</strong>{" "}
                      You can&apos;t run{" "}
                      <code className="bg-background px-1 rounded">
                        ALTER SCHEMA … SET MANAGED LOCATION
                      </code>{" "}
                      there (no External Locations / paid SKU). Either switch the
                      destination to <strong>Direct to table</strong> (works on any
                      tier), or copy the snippet from the{" "}
                      <strong>Try with Zerobus</strong> panel below and run it from
                      a Premium workspace.
                    </div>
                    <div className="border-t border-amber-300/50 pt-1 mt-1">
                      <strong className="text-amber-700">Other unsupported configurations</strong>{" "}
                      (per the{" "}
                      <a
                        href="https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-limits"
                        target="_blank" rel="noreferrer"
                        className="underline"
                      >
                        Azure Zerobus limits
                      </a>{" "}docs):
                      <ul className="list-disc pl-5 mt-1 space-y-0.5">
                        <li>
                          Workspaces with the <strong>Compliance Security Profile</strong>{" "}
                          (FedRAMP / HIPAA / PCI-DSS) — Zerobus is explicitly not
                          supported and the docs say not to use it for
                          compliance workloads.
                        </li>
                        <li>
                          Tables backed by storage <strong>secured through a private
                          endpoint</strong> — the connector can&apos;t reach them.
                        </li>
                        <li>
                          Tables with <strong>catalog commits enabled</strong> — Zerobus
                          doesn&apos;t support catalog-commit semantics.
                        </li>
                      </ul>
                      Clone-Xs can&apos;t detect these from the workspace API, so
                      this is a checklist — confirm before relying on the
                      runtime mode.
                    </div>
                  </div>
                </details>

                {/* Step 1 — Auth mode */}
                <ZerobusStep number={1} title="Choose auth mode" done={step1Done}>
                  <div className="flex gap-3 text-[12px]">
                    <label className="flex items-center gap-1 cursor-pointer">
                      <input
                        type="radio"
                        name="zb-auth-mode"
                        checked={zerobusAuthMode === "oauth"}
                        onChange={() => setZerobusAuthMode("oauth")}
                      />
                      <span>OAuth (service principal)</span>
                    </label>
                    <label className="flex items-center gap-1 cursor-pointer">
                      <input
                        type="radio"
                        name="zb-auth-mode"
                        checked={zerobusAuthMode === "pat"}
                        onChange={() => setZerobusAuthMode("pat")}
                      />
                      <span>PAT (logged-in user)</span>
                    </label>
                  </div>
                  {zerobusAuthMode === "pat" && (
                    <p className="text-[11px] text-amber-600">
                      Zerobus' server may still reject PATs that lack the right
                      scopes. If you get{" "}
                      <code className="bg-background px-1 rounded">invalid_client</code>{" "}
                      with PAT mode, switch back to OAuth and supply an SP.
                    </p>
                  )}
                </ZerobusStep>

                {/* Step 2 — Server endpoint (with derive helper). */}
                <ZerobusStep number={2} title="Set the Zerobus server endpoint" done={step2Done}>
                  <div className="space-y-1">
                    <label className="text-[11px] text-muted-foreground" htmlFor="zb-derive-url">
                      Don't know it? Paste your workspace URL and let us derive it:
                    </label>
                    <div className="flex gap-2">
                      <Input
                        id="zb-derive-url"
                        placeholder="https://dbc-….cloud.databricks.com/?o=… (or Azure / GCP equivalent)"
                        value={zerobusDeriveUrl}
                        onChange={(e) => setZerobusDeriveUrl(e.target.value)}
                        onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); deriveZerobusEndpoint(); } }}
                        className="flex-1"
                      />
                      <Button
                        size="sm"
                        onClick={deriveZerobusEndpoint}
                        disabled={zerobusDeriving || !zerobusDeriveUrl.trim()}
                      >
                        {zerobusDeriving ? <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" /> : <Zap className="h-3.5 w-3.5 mr-1" />}
                        {zerobusDeriving ? "Resolving..." : "Derive endpoint"}
                      </Button>
                    </div>
                    {zerobusDeriveError && (
                      <p className="text-[11px] text-amber-600">{zerobusDeriveError}</p>
                    )}
                    <p className="text-[10px] text-muted-foreground">
                      AWS workspaces only expose the workspace ID after login —
                      open any page in the workspace and copy the URL with
                      <code className="bg-background px-1 rounded mx-0.5">?o=…</code>
                      appended.
                    </p>
                  </div>
                  <div className="space-y-1">
                    <label className="text-[11px] text-muted-foreground" htmlFor="zb-endpoint">
                      Server endpoint
                    </label>
                    <Input
                      id="zb-endpoint"
                      placeholder="https://<wsid>.zerobus.<region>.cloud.databricks.com"
                      value={zerobusServerEndpoint}
                      onChange={(e) => setZerobusServerEndpoint(e.target.value)}
                    />
                  </div>
                </ZerobusStep>

                {/* Step 3 — Credentials. Shape depends on auth mode:
                    OAuth wants client_id + client_secret; PAT auto-lifts
                    the token off the logged-in WorkspaceClient. */}
                <ZerobusStep
                  number={3}
                  title={zerobusAuthMode === "oauth" ? "Service principal credentials" : "PAT (auto-lifted)"}
                  done={step3Done}
                >
                  {zerobusAuthMode === "oauth" ? (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                      <div className="space-y-1">
                        <label className="text-[11px] text-muted-foreground" htmlFor="zb-client-id">
                          Client ID
                        </label>
                        <Input
                          id="zb-client-id"
                          placeholder="service-principal app id"
                          value={zerobusClientId}
                          onChange={(e) => setZerobusClientId(e.target.value)}
                        />
                      </div>
                      <div className="space-y-1">
                        <label className="text-[11px] text-muted-foreground" htmlFor="zb-client-secret">
                          Client secret
                        </label>
                        <Input
                          id="zb-client-secret"
                          type="password"
                          placeholder="service-principal secret"
                          value={zerobusClientSecret}
                          onChange={(e) => setZerobusClientSecret(e.target.value)}
                          autoComplete="new-password"
                        />
                      </div>
                    </div>
                  ) : (
                    <p className="text-[11px] text-muted-foreground">
                      The Zerobus SDK call uses your logged-in PAT directly via a
                      custom <code className="bg-background px-1 rounded">HeadersProvider</code>.
                      No fields to fill in here.
                    </p>
                  )}
                </ZerobusStep>

                {/* Step 4 — Verify (OAuth only; PAT has no equivalent). */}
                {showVerifyStep && (
                  <ZerobusStep
                    number={4}
                    title="Verify credentials"
                    done={step4Done}
                    optional
                  >
                    <div className="flex items-center gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={verifyZerobusCredentials}
                        disabled={zerobusVerifying}
                      >
                        {zerobusVerifying
                          ? <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                          : <CheckCircle2 className="h-3.5 w-3.5 mr-1" />}
                        {zerobusVerifying ? "Verifying..." : "Verify credentials"}
                      </Button>
                      <span className="text-[10px] text-muted-foreground">
                        Tests the OAuth client_credentials exchange — same call the SDK does internally.
                      </span>
                    </div>
                  </ZerobusStep>
                )}

                {/* Step (4 or 5) — Optional catalog storage location. */}
                <ZerobusStep
                  number={storageStepNumber}
                  title="Catalog storage location"
                  done={!!zerobusCatalogLocation.trim()}
                  optional
                >
                  <Input
                    id="zb-catalog-loc"
                    placeholder="abfss://container@account.dfs.core.windows.net/path  ·  s3://bucket/path  ·  gs://bucket/path"
                    value={zerobusCatalogLocation}
                    onChange={(e) => setZerobusCatalogLocation(e.target.value)}
                    className="font-mono text-xs"
                  />
                  <p className="text-[10px] text-muted-foreground">
                    Required only when CREATE CATALOG fails with{" "}
                    <code className="bg-background px-1 rounded">INVALID_STATE</code> —
                    workspaces without a metastore default storage root need an explicit{" "}
                    <code className="bg-background px-1 rounded">MANAGED LOCATION</code>.
                    A UC external location / storage credential must already cover the path.
                    Ignored when the catalog already exists.
                  </p>
                </ZerobusStep>

                {/* Verify result — kept outside the Step 4 button block
                    so a long failure hint doesn't push the next step out
                    of view, but rendered just below it visually. */}
                {showVerifyStep && zerobusVerifyResult && (
                  zerobusVerifyResult.ok ? (
                    <div className="text-[11px] text-green-600 flex items-center gap-1 ml-9">
                      <CheckCircle2 className="h-3.5 w-3.5" />
                      Credentials valid — Databricks issued a token successfully.
                    </div>
                  ) : (
                    <div className="text-[11px] text-amber-600 space-y-1 border border-amber-200 rounded-md px-2 py-1.5 bg-amber-50/50 ml-9">
                      <div className="flex items-start gap-1">
                        <XCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
                        <div>
                          <span className="font-medium">
                            Failed{zerobusVerifyResult.status_code ? ` (HTTP ${zerobusVerifyResult.status_code})` : ""}
                          </span>
                          {zerobusVerifyResult.error && <>: {zerobusVerifyResult.error}</>}
                        </div>
                      </div>
                      {zerobusVerifyResult.hint && (
                        <div className="pl-4 text-muted-foreground">{zerobusVerifyResult.hint}</div>
                      )}
                    </div>
                  )
                )}
              </div>
              );
            })()}

            {/* Device profile */}
            <div className="space-y-1.5">
              <FieldLabel>Device profile</FieldLabel>
              <select className="block w-full md:w-72 h-9 px-3 text-sm bg-background border border-input rounded-md"
                value={streamProfile}
                onChange={(e) => setStreamProfile(e.target.value as typeof streamProfile)}>
                <option value="generic_sensor">Generic IoT Sensor</option>
                <option value="industrial_machine">Industrial Machine</option>
                <option value="car_obd2">Car OBD-II</option>
                <option value="smart_meter">Smart Meter (Energy)</option>
                <option value="wearable_health">Wearable Health (Healthcare)</option>
                <option value="pos_terminal">POS Terminal (Retail)</option>
                <option value="wind_turbine">Wind Turbine (Energy)</option>
                <option value="atm_transaction">ATM Transaction (Financial)</option>
                <option value="server_metrics">Server Metrics (Infra)</option>
                <option value="clickstream">Web Clickstream (Digital)</option>
              </select>
            </div>

            {/* Catalog + Schema + Volume — all three accept either an existing
                name (dropdown) or a new name ("Custom name…" → free text).
                The runner CREATE … IF NOT EXISTS each one. */}
            <div>
              <FieldLabel hint="Pick an existing catalog/schema/volume from the dropdowns, or choose 'Custom name…' on any of them to create a new one. The runner runs CREATE … IF NOT EXISTS for catalog, schema, and volume.">
                Target location
              </FieldLabel>
              <div className="mt-1 grid grid-cols-1 md:grid-cols-3 gap-3">
                {/* Catalog */}
                <div>
                  <label className="block text-xs text-muted-foreground mb-1">Catalog</label>
                  <select
                    className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md"
                    value={streamCatalogCustom ? "__custom__" : streamCatalog}
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v === "__custom__") {
                        setStreamCatalogCustom(true);
                      } else {
                        setStreamCatalogCustom(false);
                        setStreamCatalog(v);
                        // Reset schema custom flag when catalog changes — we
                        // can't know if the existing schema name applies.
                        setStreamSchemaCustom(false);
                      }
                    }}
                  >
                    <option value="">{catalogsQuery.isLoading ? "Loading…" : "Select catalog…"}</option>
                    {(catalogsQuery.data || []).map((c) => (
                      <option key={c} value={c}>{c}</option>
                    ))}
                    <option value="__custom__">Custom name… (create new)</option>
                  </select>
                  {streamCatalogCustom && (
                    <Input
                      value={streamCatalog}
                      onChange={(e) => setStreamCatalog(e.target.value)}
                      placeholder="my_catalog"
                      className="mt-1.5"
                      autoFocus
                    />
                  )}
                </div>

                {/* Schema */}
                <div>
                  <label className="block text-xs text-muted-foreground mb-1">Schema</label>
                  <select
                    className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md"
                    value={streamSchemaCustom ? "__custom__" : streamSchema}
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v === "__custom__") {
                        setStreamSchemaCustom(true);
                      } else {
                        setStreamSchemaCustom(false);
                        setStreamSchema(v);
                      }
                    }}
                    disabled={!streamCatalog && !streamCatalogCustom}
                  >
                    <option value="">
                      {!streamCatalog && !streamCatalogCustom
                        ? "Select catalog first"
                        : streamCatalogCustom
                          ? "—"
                          : (schemasQuery.isLoading ? "Loading…" : "Select schema…")}
                    </option>
                    {!streamCatalogCustom && (schemasQuery.data || []).map((s) => (
                      <option key={s} value={s}>{s}</option>
                    ))}
                    <option value="__custom__">Custom name… (create new)</option>
                  </select>
                  {streamSchemaCustom && (
                    <Input
                      value={streamSchema}
                      onChange={(e) => setStreamSchema(e.target.value)}
                      placeholder="my_schema"
                      className="mt-1.5"
                      autoFocus
                    />
                  )}
                </div>

                {/* Volume picker (volume / volume_bronze modes) OR Bronze
                    table name (direct_table mode). The third column slot
                    serves both purposes — what's shown depends on the
                    Destination radio above. */}
                <div>
                  {streamDestination === "direct_table" ? (
                    <>
                      <label className="block text-xs text-muted-foreground mb-1">Bronze table</label>
                      <Input
                        value={streamBronzeTable}
                        onChange={(e) => setStreamBronzeTable(e.target.value)}
                        placeholder={`bronze_${streamProfile}`}
                      />
                      <p className="text-[10px] text-muted-foreground mt-1">
                        Delta table created in the chosen schema. Empty → defaults to <code className="text-[10px] bg-muted/50 px-1 rounded">bronze_{streamProfile}</code>. Each tick INSERTs one batch directly.
                      </p>
                    </>
                  ) : (
                    <>
                      <label className="block text-xs text-muted-foreground mb-1">Volume</label>
                      {(() => {
                        const matches = (volumesQuery.data || [])
                          .filter((v) => (!streamCatalog || v.catalog === streamCatalog)
                                      && (!streamSchema || v.schema === streamSchema))
                          .map((v) => v.name);
                        const uniqueExisting = Array.from(new Set(matches));
                        return (
                          <>
                            <select
                              className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md"
                              value={streamVolumeCustom ? "__custom__" : streamVolume}
                              onChange={(e) => {
                                const v = e.target.value;
                                if (v === "__custom__") {
                                  setStreamVolumeCustom(true);
                                } else {
                                  setStreamVolumeCustom(false);
                                  setStreamVolume(v);
                                }
                              }}
                            >
                              <option value="events_volume">events_volume (default — created if missing)</option>
                              {uniqueExisting
                                .filter((n) => n !== "events_volume")
                                .map((name) => (
                                  <option key={name} value={name}>{name}</option>
                                ))}
                              <option value="__custom__">Custom name…</option>
                            </select>
                            {streamVolumeCustom && (
                              <Input
                                value={streamVolume}
                                onChange={(e) => setStreamVolume(e.target.value)}
                                placeholder="my_volume"
                                className="mt-1.5"
                                autoFocus
                              />
                            )}
                            <p className="text-[10px] text-muted-foreground mt-1">
                              {volumesQuery.isLoading
                                ? "Loading volumes…"
                                : streamCatalog && streamSchema && uniqueExisting.length === 0
                                  ? `No existing volumes in ${streamCatalog}.${streamSchema}. Default will be created on start.`
                                  : `${uniqueExisting.length} existing volume${uniqueExisting.length === 1 ? "" : "s"} in scope.`}
                            </p>
                          </>
                        );
                      })()}
                    </>
                  )}
                </div>
              </div>
            </div>

            {/* Cadence */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-1.5">
                <FieldLabel>Events per batch</FieldLabel>
                <Input type="number"
                  min={streamLimits.events_per_batch.min}
                  max={streamLimits.events_per_batch.max}
                  value={streamEventsPerBatch}
                  onChange={(e) => {
                    const { min, max, default: dflt } = streamLimits.events_per_batch;
                    setStreamEventsPerBatch(Math.max(min, Math.min(max, parseInt(e.target.value) || dflt)));
                  }} />
              </div>
              <div className="space-y-1.5">
                <FieldLabel>Interval (seconds)</FieldLabel>
                <Input type="number"
                  min={streamLimits.interval_seconds.min}
                  max={streamLimits.interval_seconds.max}
                  value={streamIntervalSeconds}
                  onChange={(e) => {
                    // UI rounds to whole seconds via parseInt; the API
                    // accepts fractional via direct calls (see the YAML
                    // comment on streaming_limits.interval_seconds).
                    const { min, max, default: dflt } = streamLimits.interval_seconds;
                    const lo = Math.max(1, Math.ceil(min));
                    setStreamIntervalSeconds(Math.max(lo, Math.min(max, parseInt(e.target.value) || dflt)));
                  }} />
              </div>
              <div className="space-y-1.5">
                <FieldLabel>Total duration (seconds, max {streamLimits.total_duration_seconds.max})</FieldLabel>
                <Input type="number"
                  min={streamLimits.total_duration_seconds.min}
                  max={streamLimits.total_duration_seconds.max}
                  value={streamDurationSeconds}
                  onChange={(e) => {
                    const { min, max, default: dflt } = streamLimits.total_duration_seconds;
                    setStreamDurationSeconds(Math.max(min, Math.min(max, parseInt(e.target.value) || dflt)));
                  }} />
              </div>
            </div>

            {/* Bronze refresh cadence — only meaningful when destination
                is volume_bronze (the runner runs CREATE OR REFRESH
                STREAMING TABLE on this CRON). Hidden in volume-only and
                direct_table modes. */}
            {streamDestination === "volume_bronze" && (
              <div className="border border-dashed border-border rounded-md p-3 bg-muted/20">
                <p className="text-xs">
                  Bronze table <code className="text-[10px] bg-muted/50 px-1 rounded">bronze_{streamProfile}</code> will be created via <code className="text-[10px] bg-muted/50 px-1 rounded">CREATE OR REFRESH STREAMING TABLE</code> on the cadence below. Requires DBSQL Serverless that supports the syntax (Free Edition does — uses CRON form).
                </p>
                <div className="mt-3 flex items-center gap-3">
                  <span className="text-xs text-muted-foreground">Refresh every</span>
                  <Input type="number" min={1} max={60} value={streamBronzeRefreshMinutes}
                    onChange={(e) => setStreamBronzeRefreshMinutes(Math.max(1, Math.min(60, parseInt(e.target.value) || 5)))}
                    className="w-20 h-7 text-xs px-2.5" />
                  <span className="text-xs text-muted-foreground">minutes</span>
                </div>
              </div>
            )}

            {/* Action buttons */}
            <div className="flex items-center gap-2">
              <Button onClick={handleStartStreaming}
                disabled={streamingEmit.isPending || (!!streamingJob && streamingJob.status === "running")}>
                {streamingEmit.isPending
                  ? <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  : <Play className="h-4 w-4 mr-2" />}
                Start streaming
              </Button>
              {streamingJob?.status === "running" && (
                <Button variant="outline" onClick={handleStopStreaming}
                  disabled={streamingStop.isPending}>
                  <StopCircle className="h-4 w-4 mr-2" />Stop
                </Button>
              )}
              {/* Sibling to Start — opens a modal that creates a real
                  Databricks Job. The in-process Start is unchanged;
                  users only click this when they need unattended runs. */}
              <Button variant="outline" onClick={() => {
                if (!streamCatalog.trim() || !streamSchema.trim()) {
                  toast.error("Catalog and schema are required");
                  return;
                }
                setScheduleResult(null);
                setScheduleName(`clxs-stream-${streamProfile}`);
                setScheduleModalOpen(true);
              }}>
                <Calendar className="h-4 w-4 mr-2" />Schedule on Databricks
              </Button>
            </div>

            {/* Live progress */}
            {streamingJob && (
              <Card className="bg-card border-border">
                <CardContent className="pt-4 space-y-2">
                  <div className="flex items-center gap-2">
                    {streamingJob.status === "completed" ? <CheckCircle2 className="h-4 w-4 text-green-500" />
                      : streamingJob.status === "failed" ? <XCircle className="h-4 w-4 text-red-500" />
                      : <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
                    <span className="text-sm font-medium">
                      {streamingJob.status === "running" ? "Emitting..."
                        : streamingJob.status === "completed" ? "Completed"
                        : streamingJob.status === "failed" ? "Failed"
                        : streamingJob.status}
                    </span>
                    <span className="text-xs text-muted-foreground ml-auto">Job {streamingJobId}</span>
                  </div>
                  {streamingJob.progress && (() => {
                    const elapsed = Number(streamingJob.progress.elapsed_seconds ?? 0);
                    const events = Number(streamingJob.progress.events_emitted ?? 0);
                    const filesWritten = Number(streamingJob.progress.files_written ?? 0);
                    const rowsInserted = Number(streamingJob.progress.rows_inserted ?? 0);
                    // Throughput: cumulative events / wall-clock elapsed.
                    // Bias the divisor to >=1s so the rate doesn't blow up
                    // on the first sub-second tick.
                    const eventsPerSec = elapsed > 0
                      ? (events / Math.max(elapsed, 1)).toFixed(1)
                      : "—";
                    // Time remaining is derived from the form-controlled
                    // total duration. Only shown while running, and only
                    // when emission hasn't already overrun (rare but
                    // possible with a slow warehouse).
                    const isRunning = streamingJob.status === "running";
                    const remaining = isRunning && streamDurationSeconds > elapsed
                      ? Math.max(0, Math.round(streamDurationSeconds - elapsed))
                      : null;
                    // "Rows inserted: 0" is a frequent FAQ in volume_bronze
                    // mode — rows are inserted by the bronze streaming
                    // table's refresh job, not the emitter. Surface that
                    // inline with a small Info icon + native title tooltip
                    // when the situation is exactly that (files landed,
                    // rows haven't yet).
                    const showRowsHint =
                      streamDestination === "volume_bronze"
                      && rowsInserted === 0
                      && filesWritten > 0;
                    // Per-tick error bubbled up from the runner's
                    // try/except so the UI doesn't silently report
                    // "Completed — 0 events" when every tick threw.
                    // The user previously had to comb the API server
                    // logs to see the real cause.
                    const tickErrors = Number(streamingJob.progress.tick_errors ?? 0);
                    const lastError = (streamingJob.progress.last_error ?? "") as string;
                    return (
                      <>
                        <div className="grid grid-cols-2 md:grid-cols-6 gap-2 text-xs">
                          <div><span className="text-muted-foreground">Events emitted:</span> <span className="font-mono">{events}</span></div>
                          <div><span className="text-muted-foreground">Files written:</span> <span className="font-mono">{filesWritten}</span></div>
                          <div className="flex items-center gap-1">
                            <span className="text-muted-foreground">Rows inserted:</span>
                            <span className="font-mono">{rowsInserted}</span>
                            {showRowsHint && (
                              <Info
                                className="h-3 w-3 text-muted-foreground cursor-help"
                                aria-label="Rows-inserted explainer"
                                title="Rows are inserted by the bronze streaming table's refresh job, not by this emitter. They'll appear after the next refresh cycle (configured above)."
                              />
                            )}
                          </div>
                          <div><span className="text-muted-foreground">Ticks:</span> <span className="font-mono">{streamingJob.progress.ticks ?? 0}</span></div>
                          <div><span className="text-muted-foreground">Events/s:</span> <span className="font-mono">{eventsPerSec}</span></div>
                          <div>
                            <span className="text-muted-foreground">Elapsed:</span>{" "}
                            <span className="font-mono">{elapsed}s</span>
                            {remaining !== null && (
                              <span className="ml-1 text-[10px] text-muted-foreground">
                                · {remaining}s left
                              </span>
                            )}
                          </div>
                        </div>
                        {tickErrors > 0 && lastError && (
                          <div className="mt-2 border border-amber-300 rounded-md px-2 py-1.5 bg-amber-50/50 text-[11px] space-y-0.5">
                            <div className="text-amber-700">
                              <strong>{tickErrors}</strong> tick{tickErrors === 1 ? "" : "s"} failed.
                              Last error:
                            </div>
                            <code className="block font-mono text-amber-900 whitespace-pre-wrap break-words">
                              {lastError}
                            </code>
                          </div>
                        )}
                      </>
                    );
                  })()}

                  {/* Throughput chart — dual-axis composed chart of
                      cumulative events (left, area-filled) and per-tick
                      delta (right, dashed line) over elapsed seconds.
                      A horizontal dashed reference line on the right axis
                      marks the configured `events_per_batch` (what each
                      tick should land if the runner is keeping up).
                      Red dots on the cumulative line mark ticks where
                      `tick_errors` increased. Hidden until we have ≥2
                      samples (one point isn't a line). */}
                  {streamingSeries.length >= 2 && (() => {
                    // Peak per-tick events seen in the run — used to
                    // decide whether the "expected /tick" reference line
                    // is meaningful. If the form value is < 1% of the
                    // peak delta (e.g. user changed the form to 100 after
                    // running with 1M batches) the line is essentially at
                    // the X-axis and the label collides with the last
                    // X-tick. Hide it in that case.
                    const maxDelta = streamingSeries.reduce(
                      (m: number, p: any) => Math.max(m, Number(p?.delta) || 0),
                      0,
                    );
                    const showExpectedLine =
                      streamEventsPerBatch > 0 &&
                      maxDelta > 0 &&
                      streamEventsPerBatch >= maxDelta * 0.01;
                    return (
                    <div className="border border-border rounded-md bg-background p-2 mt-2">
                      <div className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide mb-1">Throughput</div>
                      <ResponsiveContainer width="100%" height={220}>
                        {/* bottom margin: 30 — leaves room for the X-axis
                            tick row + "elapsed (s)" label + the Legend
                            without any overlap. The previous 18 was tight
                            and "elapsed (s)" sat almost on top of the
                            legend at narrow widths. */}
                        <ComposedChart data={streamingSeries} margin={{ top: 8, right: 24, bottom: 30, left: 8 }}>
                          {/* Subtle area fill under the cumulative line.
                              Defined as a linearGradient so the alpha
                              fades to ~0 at the bottom — gives the line
                              visual weight without dominating the chart. */}
                          <defs>
                            <linearGradient id="cumulativeFill" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%" stopColor="#E8453C" stopOpacity={0.25} />
                              <stop offset="100%" stopColor="#E8453C" stopOpacity={0.02} />
                            </linearGradient>
                          </defs>
                          <CartesianGrid strokeDasharray="3 3" stroke="currentColor" className="text-muted-foreground/20" />
                          <XAxis
                            dataKey="elapsed"
                            type="number"
                            domain={[0, "dataMax"]}
                            tick={{ fontSize: 10, fill: "currentColor" }}
                            stroke="currentColor"
                            className="text-muted-foreground"
                            label={{
                              value: "elapsed (s)",
                              // Render the X-axis title BELOW the tick row
                              // (negative offset = further from chart),
                              // and let the Legend sit below it via the
                              // increased bottom margin. Previously
                              // "insideBottom" + small offset put the
                              // title flush against the tick labels.
                              position: "insideBottom",
                              offset: -16,
                              style: { fontSize: 10, fill: "currentColor" },
                              className: "text-muted-foreground",
                            }}
                          />
                          <YAxis
                            yAxisId="cumulative"
                            tick={{ fontSize: 10, fill: "currentColor" }}
                            stroke="currentColor"
                            className="text-muted-foreground"
                            allowDecimals={false}
                            tickFormatter={fmtN}
                            width={48}
                            label={{
                              value: "cumulative",
                              angle: -90,
                              position: "insideLeft",
                              offset: 10,
                              style: { fontSize: 10, fill: "currentColor", textAnchor: "middle" },
                              className: "text-muted-foreground",
                            }}
                          />
                          <YAxis
                            yAxisId="delta"
                            orientation="right"
                            tick={{ fontSize: 10, fill: "currentColor" }}
                            stroke="currentColor"
                            className="text-muted-foreground"
                            allowDecimals={false}
                            tickFormatter={fmtN}
                            width={48}
                            label={{
                              value: "per tick",
                              angle: 90,
                              position: "insideRight",
                              offset: 10,
                              style: { fontSize: 10, fill: "currentColor", textAnchor: "middle" },
                              className: "text-muted-foreground",
                            }}
                          />
                          <Tooltip
                            contentStyle={{
                              background: "var(--popover, var(--card, #2C2C2C))",
                              border: "1px solid var(--border, #404040)",
                              borderRadius: 8,
                              fontSize: 11,
                              color: "var(--popover-foreground, var(--card-foreground, #fff))",
                            }}
                            // dataKey-based naming so the legend label
                            // and tooltip label always match — fixes the
                            // earlier bug where both rows said "Events /
                            // tick" because the formatter checked `name`
                            // (the legend label) instead of the dataKey.
                            formatter={(v: number, _name: string, item: any) => {
                              const key = item?.dataKey;
                              const label = key === "events"
                                ? "Cumulative events"
                                : key === "delta"
                                  ? "Events / tick"
                                  : String(_name);
                              return [fmtN(v), label];
                            }}
                            labelFormatter={(v: number) => `t=${v}s`}
                          />
                          {/* Legend pinned to the very bottom of the
                              wrapper so the X-axis title ("elapsed (s)")
                              sits ABOVE it inside the bottom margin —
                              previously the two crowded each other. */}
                          <Legend
                            verticalAlign="bottom"
                            align="center"
                            wrapperStyle={{ fontSize: 10, paddingTop: 8, position: "relative", marginTop: 2 }}
                          />
                          {/* Reference line at the configured events_per_batch
                              on the per-tick axis — visualises "this is what
                              each tick should land if we're keeping up".
                              Hidden when the configured value is < 1% of
                              peak delta (e.g. user changed the form value
                              after running) — at that scale the line
                              renders flush against the X-axis and the
                              label collides with the last X-tick. */}
                          {showExpectedLine && (
                            <ReferenceLine
                              yAxisId="delta"
                              y={streamEventsPerBatch}
                              stroke="currentColor"
                              className="text-muted-foreground"
                              strokeDasharray="2 4"
                              strokeOpacity={0.5}
                              label={{
                                value: `expected ${fmtN(streamEventsPerBatch)}/tick`,
                                // "insideTopLeft" places the label inside
                                // the chart area at the top-left of the
                                // line — far from the bottom-right corner
                                // where the last X-axis tick lives, so the
                                // two never collide regardless of where
                                // the line ends up vertically.
                                position: "insideTopLeft",
                                fill: "currentColor",
                                fontSize: 9,
                                className: "text-muted-foreground",
                              }}
                            />
                          )}
                          <Area
                            yAxisId="cumulative"
                            type="monotone"
                            dataKey="events"
                            name="Cumulative events"
                            stroke="#E8453C"
                            strokeWidth={2}
                            fill="url(#cumulativeFill)"
                            dot={false}
                            // Render a red ⨯-style dot at any tick where
                            // tick_errors incremented. Using activeDot is
                            // wrong here (it only fires on hover); we want
                            // the marker to be persistent.
                            isAnimationActive={false}
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            activeDot={{ r: 4, stroke: "#E8453C", strokeWidth: 2, fill: "var(--background, #fff)" }}
                          />
                          <Line
                            yAxisId="delta"
                            type="monotone"
                            dataKey="delta"
                            name="Events / tick"
                            stroke="currentColor"
                            className="text-muted-foreground"
                            strokeWidth={1.5}
                            strokeDasharray="3 3"
                            dot={false}
                            isAnimationActive={false}
                          />
                          {/* Error markers — render as a separate Line
                              with hidden stroke and a custom dot that
                              only appears when payload.hasError is true.
                              A separate series keeps the visual layer
                              decoupled from the cumulative Area. */}
                          <Line
                            yAxisId="cumulative"
                            type="monotone"
                            dataKey="events"
                            name="Tick errors"
                            stroke="transparent"
                            strokeWidth={0}
                            isAnimationActive={false}
                            legendType="none"
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            dot={(props: any) => {
                              if (!props?.payload?.hasError) {
                                // Recharts requires a valid SVG element
                                // here, not null — a zero-radius circle
                                // is the cheapest no-op.
                                return <circle key={`no-err-${props?.index ?? 0}`} cx={props?.cx} cy={props?.cy} r={0} />;
                              }
                              return (
                                <g key={`err-${props.index}`} transform={`translate(${props.cx},${props.cy})`}>
                                  <circle r={4} fill="#dc2626" stroke="var(--background, #fff)" strokeWidth={1.5} />
                                  <line x1={-2} y1={-2} x2={2} y2={2} stroke="var(--background, #fff)" strokeWidth={1.5} />
                                  <line x1={-2} y1={2} x2={2} y2={-2} stroke="var(--background, #fff)" strokeWidth={1.5} />
                                </g>
                              );
                            }}
                          />
                        </ComposedChart>
                      </ResponsiveContainer>
                    </div>
                    );
                  })()}
                  {streamingJob.progress?.current_batch_path && (
                    <div className="flex items-center gap-1 text-[11px] text-muted-foreground font-mono">
                      <span className="truncate flex-1" title={streamingJob.progress.current_batch_path}>
                        Latest: {streamingJob.progress.current_batch_path}
                      </span>
                      <button
                        type="button"
                        onClick={() => copyToClipboard(streamingJob.progress.current_batch_path, "Path copied")}
                        className="shrink-0 p-1 rounded hover:bg-muted/50"
                        aria-label="Copy batch path"
                        title="Copy batch path"
                      >
                        <Copy className="h-3 w-3" />
                      </button>
                    </div>
                  )}
                  {/* Bronze status — only shown when result has landed */}
                  {streamingJob.result?.bronze_status === "created" && streamingJob.result?.bronze_table_fqn && (() => {
                    // Pull catalog/schema/profile off the job result rather than the form
                    // state — the form fields can be empty by the time the user clicks
                    // (e.g. after a fresh load that hydrated the job from sessionStorage
                    // but didn't restore the form).
                    const r = streamingJob.result || {};
                    const cat = r.catalog || streamCatalog;
                    const sch = r.schema || streamSchema;
                    const prof = r.profile || streamProfile;
                    const fqnQuoted = `\`${cat}\`.\`${sch}\`.\`bronze_${prof}\``;
                    // captured_at is the per-event timestamp populated by every device
                    // profile (see DEVICE_PROFILES in src/demo_streaming.py) — uniform
                    // across atm_transaction, smart_meter, car_obd2, etc.
                    const previewSql = `SELECT * FROM ${fqnQuoted} ORDER BY captured_at DESC LIMIT 10`;
                    const workbenchSql = `SELECT * FROM ${fqnQuoted} ORDER BY captured_at DESC LIMIT 100`;
                    const workbenchHref = `/data-lab#q=${btoa(encodeURIComponent(workbenchSql))}&run=1`;
                    return (
                      <div className="border-t border-border pt-2 mt-2 space-y-2 text-xs">
                        <div className="flex flex-wrap items-center gap-1">
                          <CheckCircle2 className="h-3.5 w-3.5 text-green-500 inline" />
                          <span>Bronze streaming table created:</span>
                          <code className="text-[11px] bg-muted/50 px-1 rounded">
                            {streamingJob.result.bronze_table_fqn}
                          </code>
                          <button
                            type="button"
                            onClick={() => copyToClipboard(streamingJob.result.bronze_table_fqn, "Table FQN copied")}
                            className="shrink-0 p-1 rounded hover:bg-muted/50"
                            aria-label="Copy table FQN"
                            title="Copy table FQN"
                          >
                            <Copy className="h-3 w-3" />
                          </button>
                          <a
                            href={workbenchHref}
                            className="text-[#E8453C] hover:underline ml-auto"
                          >
                            Open in workbench →
                          </a>
                        </div>
                        <StreamingPreview
                          sql={previewSql}
                          // Refetch when emission progresses — events_emitted
                          // monotonically increases, so passing it as a key
                          // dependency naturally drives a refresh per batch.
                          version={streamingJob.progress?.events_emitted ?? 0}
                          done={streamingJob.status !== "running"}
                        />
                      </div>
                    );
                  })()}
                  {streamingJob.result?.bronze_status === "failed" && (
                    <div className="border-t border-border pt-2 mt-2 text-xs text-amber-600">
                      <XCircle className="h-3.5 w-3.5 inline mr-1" />
                      Bronze auto-create failed: {streamingJob.result.bronze_error}. The Volume files still landed — run the SQL below manually after enabling DBSQL Serverless.
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Auto Loader SQL snippet — only relevant for the
                Volume → Auto Loader path. Hidden for volume-only (no
                Bronze) and direct_table (no files / no Auto Loader). */}
            {streamDestination === "volume_bronze" && (
              <div className="border border-border rounded-md bg-muted/20 p-3 space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Auto Loader SQL (copy-paste into DBSQL)</span>
                  <Button size="sm" variant="ghost" onClick={() => {
                    navigator.clipboard?.writeText(autoLoaderSnippet);
                    toast.success("Copied to clipboard");
                  }}>
                    <ClipboardCopy className="h-3.5 w-3.5 mr-1" />Copy
                  </Button>
                </div>
                <pre className="text-[11px] font-mono bg-background border border-border rounded p-2 overflow-x-auto whitespace-pre">{autoLoaderSnippet}</pre>
              </div>
            )}

            {/* Try-with-Zerobus snippet panel — independent of the
                selected destination (Zerobus is a parallel emit path,
                not tied to any of the three current modes). Lazy-fetches
                the snippet from /api/generate/demo-data/zerobus-snippet
                only when the user expands the panel, then refetches
                whenever the form values change. */}
            <ZerobusSnippetPanel
              profile={streamProfile}
              catalog={streamCatalog.trim() || "main"}
              schema={streamSchema.trim() || "iot"}
              table={`bronze_${streamProfile}`}
              eventsPerBatch={streamEventsPerBatch}
              intervalSeconds={streamIntervalSeconds}
            />
          </CardContent>
      </Card>
      </>)}

      {activeGenTab === "documents" && <DocumentsTab />}

      {activeGenTab === "media" && <MediaTab />}

      {activeGenTab === "knowledge" && <KnowledgeTab />}

      {activeGenTab === "logs" && <LogsTab />}

      {activeGenTab === "code" && <CodeTab />}

      {activeGenTab === "capture" && <LiveCaptureTab />}

      {activeGenTab === "manage" && (() => {
        const rows = (demoCatalogsQuery.data?.catalogs || []) as any[];
        return (
          <Card className="bg-card border-border">
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <Trash2 className="h-4 w-4 text-[#E8453C]" />
                Manage Catalogs
                <Badge variant="outline" className="text-[10px] ml-1">{demoCatalogsQuery.data?.total ?? 0}</Badge>
                <span className="ml-auto flex items-center gap-2 text-xs font-normal">
                  <label className="flex items-center gap-1.5 cursor-pointer">
                    <input type="checkbox"
                      checked={manageDemoOnly}
                      onChange={(e) => setManageDemoOnly(e.target.checked)}
                      className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]" />
                    Demo only
                  </label>
                  <Button size="sm" variant="ghost" onClick={() => demoCatalogsQuery.refetch()}>
                    <RefreshCw className={`h-3.5 w-3.5 ${demoCatalogsQuery.isFetching ? "animate-spin" : ""}`} />
                  </Button>
                </span>
              </CardTitle>
              <p className="text-xs text-muted-foreground mt-1">
                Lists every catalog you can read. Toggle <strong>Demo only</strong> to filter to catalogs tagged
                with <code className="text-[10px] bg-muted/50 px-1 rounded">demo.generated_by = 'clone-xs'</code>.
                Drop is permanent — confirm by typing the catalog name.
              </p>
            </CardHeader>
            <CardContent>
              {demoCatalogsQuery.isLoading ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
                  <Loader2 className="h-4 w-4 animate-spin" /> Probing catalogs...
                </div>
              ) : demoCatalogsQuery.isError ? (
                <div className="p-3 bg-red-500/5 border border-red-500/20 rounded-md text-red-500 text-sm">
                  {(demoCatalogsQuery.error as Error)?.message || "Failed to load catalogs"}
                </div>
              ) : rows.length === 0 ? (
                <div className="py-12 text-center text-muted-foreground">
                  <Database className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">{manageDemoOnly ? "No demo catalogs found" : "No catalogs visible"}</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="border-b border-border">
                      <tr className="text-left text-xs font-medium text-muted-foreground uppercase tracking-wide">
                        <th className="px-3 py-2">Catalog</th>
                        <th className="px-3 py-2">Demo?</th>
                        <th className="px-3 py-2 text-right">Schemas</th>
                        <th className="px-3 py-2 text-right">Demo Tables</th>
                        <th className="px-3 py-2 text-right">All Tables</th>
                        <th className="px-3 py-2">Owner</th>
                        <th className="px-3 py-2 w-[80px]">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={r.name} className="border-b border-border/40 hover:bg-muted/20">
                          <td className="px-3 py-2 font-mono text-sm">{r.name}</td>
                          <td className="px-3 py-2">
                            {r.is_demo
                              ? <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]"><Check className="h-3 w-3 mr-1" />Demo</Badge>
                              : <span className="text-xs text-muted-foreground">—</span>}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-xs">{r.num_schemas}</td>
                          <td className="px-3 py-2 text-right font-mono text-xs">{r.num_demo_tables}</td>
                          <td className="px-3 py-2 text-right font-mono text-xs">{r.num_tables}</td>
                          <td className="px-3 py-2 text-xs text-muted-foreground truncate max-w-[200px]" title={r.owner}>{r.owner || "—"}</td>
                          <td className="px-3 py-2">
                            <button
                              className="p-1.5 rounded hover:bg-red-500/10 text-muted-foreground hover:text-red-500 transition-colors"
                              title={`Drop catalog ${r.name}`}
                              onClick={() => { setDropModalCatalog(r.name); setDropModalTyped(""); }}>
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        );
      })()}

      {/* Typed-confirm modal — Manage tab drop. Stricter than the
          existing Batch-tab window.confirm() since this surface
          encourages bulk cleanup; we want a deliberate keystroke
          per drop, not a single OK click. */}
      {dropModalCatalog && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
          onClick={() => !demoCatalogDrop.isPending && setDropModalCatalog(null)}>
          <div className="bg-background border border-border rounded-lg shadow-xl max-w-md w-full mx-4 overflow-hidden"
            onClick={(e) => e.stopPropagation()}>
            <div className="px-5 py-3 border-b border-border flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Trash2 className="h-4 w-4 text-red-500" />
                <h3 className="text-sm font-semibold text-foreground">Drop catalog</h3>
              </div>
              <button onClick={() => !demoCatalogDrop.isPending && setDropModalCatalog(null)}
                className="text-muted-foreground hover:text-foreground"><XCircle className="h-4 w-4" /></button>
            </div>
            <div className="p-5 space-y-3">
              <p className="text-sm">
                This will execute <code className="text-[11px] bg-muted/50 px-1 rounded">DROP CATALOG {dropModalCatalog} CASCADE</code>{" "}
                and cannot be undone. Every schema, table, view, function, and volume below it will be deleted.
              </p>
              <p className="text-xs text-muted-foreground">
                Type <code className="text-[11px] bg-muted/50 px-1 rounded">{dropModalCatalog}</code> to confirm:
              </p>
              <Input value={dropModalTyped} onChange={(e) => setDropModalTyped(e.target.value)}
                placeholder={dropModalCatalog} autoFocus />
              {demoCatalogDrop.isError && (
                <div className="text-xs text-red-500">{(demoCatalogDrop.error as Error)?.message}</div>
              )}
            </div>
            <div className="px-5 py-3 border-t border-border flex items-center justify-end gap-2">
              <Button size="sm" variant="outline" onClick={() => setDropModalCatalog(null)}
                disabled={demoCatalogDrop.isPending}>Cancel</Button>
              <Button size="sm"
                disabled={dropModalTyped !== dropModalCatalog || demoCatalogDrop.isPending}
                className="bg-red-500 hover:bg-red-600 text-white"
                onClick={async () => {
                  try {
                    await demoCatalogDrop.mutateAsync({ catalog_name: dropModalCatalog });
                    toast.success(`Dropped ${dropModalCatalog}`);
                    setDropModalCatalog(null);
                  } catch (e) {
                    toast.error((e as Error).message || "Drop failed");
                  }
                }}>
                {demoCatalogDrop.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Trash2 className="h-4 w-4 mr-2" />}
                Drop catalog
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Schedule streaming on Databricks modal — collects cron +
          submits to /demo-data/streaming/schedule. On success shows
          the new Job's URL with a one-click open button. */}
      {scheduleModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
          onClick={() => !streamingSchedule.isPending && setScheduleModalOpen(false)}>
          <div className="bg-background border border-border rounded-lg shadow-xl max-w-2xl w-full mx-4 overflow-hidden"
            onClick={(e) => e.stopPropagation()}>
            <div className="px-5 py-3 border-b border-border flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-[#E8453C]" />
                <h3 className="text-sm font-semibold text-foreground">Schedule streaming on Databricks</h3>
              </div>
              <button onClick={() => !streamingSchedule.isPending && setScheduleModalOpen(false)}
                className="text-muted-foreground hover:text-foreground"><XCircle className="h-4 w-4" /></button>
            </div>
            <div className="p-5 space-y-4 max-h-[70vh] overflow-y-auto">
              {!scheduleResult ? (
                <>
                  <p className="text-xs text-muted-foreground">
                    Generates a self-contained notebook in your workspace and creates a Databricks Job
                    on the cron below. Emission runs on Databricks compute and survives API restarts.
                    The Job is tagged <code className="text-[10px] bg-muted/50 px-1 rounded">created_by=clone-xs</code> so it shows in your Jobs list.
                  </p>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <div>
                      <FieldLabel>Job name</FieldLabel>
                      <Input value={scheduleName} onChange={(e) => setScheduleName(e.target.value)}
                        placeholder={`clxs-stream-${streamProfile}`} className="mt-1" />
                    </div>
                    <div>
                      <FieldLabel>Timezone</FieldLabel>
                      <Input value={scheduleTimezone} onChange={(e) => setScheduleTimezone(e.target.value)}
                        placeholder="UTC" className="mt-1" />
                    </div>
                  </div>
                  <div>
                    <FieldLabel>Quartz cron expression</FieldLabel>
                    <Input value={scheduleCron} onChange={(e) => setScheduleCron(e.target.value)}
                      placeholder="0 */5 * * * ?" className="mt-1 font-mono text-xs" />
                    <div className="flex items-center gap-1 mt-1.5 flex-wrap">
                      <span className="text-[10px] text-muted-foreground">Quick picks:</span>
                      {[
                        { label: "Every 5 min", cron: "0 */5 * * * ?" },
                        { label: "Top of hour", cron: "0 0 * * * ?" },
                        { label: "Weekdays 9am", cron: "0 0 9 ? * MON-FRI" },
                      ].map((p) => (
                        <button key={p.cron} onClick={() => setScheduleCron(p.cron)}
                          className="text-[10px] text-[#E8453C] hover:underline px-1.5 py-0.5 rounded">
                          {p.label}
                        </button>
                      ))}
                    </div>
                  </div>
                  <label className="flex items-center gap-2 text-sm">
                    <input type="checkbox" checked={scheduleUseServerless}
                      onChange={(e) => setScheduleUseServerless(e.target.checked)}
                      className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]" />
                    Use Serverless compute (recommended)
                  </label>
                  <div>
                    <FieldLabel>Notebook path (advanced)</FieldLabel>
                    <Input value={scheduleNotebookPath} onChange={(e) => setScheduleNotebookPath(e.target.value)}
                      placeholder={`/Users/<me>/clxs/streaming_${streamProfile}_<timestamp>`}
                      className="mt-1 font-mono text-xs" />
                    <p className="text-[10px] text-muted-foreground mt-1">
                      Leave empty for the default per-user, timestamped path. Existing notebooks at the
                      same path are overwritten.
                    </p>
                  </div>
                  {streamingSchedule.isError && (
                    <div className="text-xs text-red-500 p-2 bg-red-500/5 border border-red-500/20 rounded">
                      {(streamingSchedule.error as Error).message}
                    </div>
                  )}
                </>
              ) : (
                <div className="space-y-3">
                  <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                    Job created
                  </div>
                  <dl className="text-xs space-y-1.5">
                    <div className="flex gap-2">
                      <dt className="text-muted-foreground w-32 shrink-0">Job ID:</dt>
                      <dd className="font-mono">{scheduleResult.job_id}</dd>
                    </div>
                    <div className="flex gap-2">
                      <dt className="text-muted-foreground w-32 shrink-0">Schedule:</dt>
                      <dd className="font-mono">{scheduleResult.schedule_quartz_cron} ({scheduleResult.timezone_id})</dd>
                    </div>
                    <div className="flex gap-2">
                      <dt className="text-muted-foreground w-32 shrink-0">Notebook:</dt>
                      <dd className="font-mono text-[11px] break-all">{scheduleResult.notebook_path}</dd>
                    </div>
                    {scheduleResult.bronze_status && (
                      <div className="flex gap-2">
                        <dt className="text-muted-foreground w-32 shrink-0">Bronze table:</dt>
                        <dd className="text-[11px] flex-1">
                          {scheduleResult.bronze_status === "created" && (
                            <span className="inline-flex items-center gap-1 text-green-600">
                              <CheckCircle2 className="h-3 w-3" />
                              <code className="bg-muted/50 px-1 rounded font-mono">
                                {scheduleResult.bronze_table_fqn}
                              </code>
                            </span>
                          )}
                          {scheduleResult.bronze_status === "failed" && (
                            <span className="inline-flex items-start gap-1 text-amber-600">
                              <XCircle className="h-3 w-3 mt-0.5 shrink-0" />
                              <span>
                                Bronze auto-create failed: {scheduleResult.bronze_error}.
                                Files will still land in the volume; create the table manually after enabling DBSQL Serverless.
                              </span>
                            </span>
                          )}
                          {scheduleResult.bronze_status === "skipped" && (
                            <span className="inline-flex items-start gap-1 text-amber-600">
                              <Info className="h-3 w-3 mt-0.5 shrink-0" />
                              <span>Bronze table not created: {scheduleResult.bronze_error}</span>
                            </span>
                          )}
                        </dd>
                      </div>
                    )}
                  </dl>
                  <a href={scheduleResult.run_url} target="_blank" rel="noreferrer"
                    className="inline-flex items-center gap-1.5 text-xs text-[#E8453C] hover:underline">
                    <ExternalLink className="h-3.5 w-3.5" />Open in Databricks Jobs
                  </a>
                </div>
              )}
            </div>
            <div className="px-5 py-3 border-t border-border flex items-center justify-end gap-2">
              <Button size="sm" variant="outline"
                onClick={() => setScheduleModalOpen(false)}
                disabled={streamingSchedule.isPending}>
                {scheduleResult ? "Close" : "Cancel"}
              </Button>
              {!scheduleResult && (
                <Button size="sm"
                  disabled={streamingSchedule.isPending || !scheduleCron.trim()}
                  onClick={async () => {
                    try {
                      const res = await streamingSchedule.mutateAsync({
                        catalog: streamCatalog.trim(),
                        schema: streamSchema.trim(),
                        volume: streamVolume.trim() || "events_volume",
                        profile: streamProfile,
                        events_per_batch: streamEventsPerBatch,
                        interval_seconds: streamIntervalSeconds,
                        total_duration_seconds: streamDurationSeconds,
                        auto_create_bronze: streamDestination === "volume_bronze",
                        bronze_refresh_minutes: streamBronzeRefreshMinutes,
                        name: scheduleName.trim() || undefined,
                        schedule_quartz_cron: scheduleCron.trim(),
                        timezone_id: scheduleTimezone.trim() || "UTC",
                        notebook_path: scheduleNotebookPath.trim() || undefined,
                        use_serverless: scheduleUseServerless,
                      });
                      setScheduleResult(res);
                      toast.success(`Scheduled (Job ${res.job_id})`);
                    } catch (e) {
                      toast.error((e as Error).message || "Schedule failed");
                    }
                  }}>
                  {streamingSchedule.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Calendar className="h-4 w-4 mr-2" />}
                  Create scheduled Job
                </Button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


// Inline preview of the most-recently-emitted rows in the bronze table.
//
// The schema chips above the table are derived from the first response's
// keys — they cost nothing extra (we already SELECT *), and they let the
// user confirm field names without leaving the page.
//
// `version` is bumped each emission tick (events_emitted) so React's
// dependency array refetches naturally on each new batch. Failures are
// rendered inline (rather than thrown as toasts) because warehouse-cold-
// start is the most common cause and that's expected to clear within a
// few seconds — toast spam would be worse than a quiet inline message.
function StreamingPreview({
  sql,
  version,
  done,
}: {
  sql: string;
  version: number;
  done: boolean;
}) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api.post<Record<string, unknown>[]>("/execute-sql", { sql })
      .then((data) => {
        if (cancelled) return;
        setRows(Array.isArray(data) ? data : []);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e?.message ?? "preview unavailable");
        setRows([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [sql, version]);

  if (loading && rows.length === 0) {
    return (
      <div className="text-[11px] text-muted-foreground italic">
        Loading preview...
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-[11px] text-amber-600">
        Preview unavailable: {error}
        {!done && " (will retry on next batch)"}
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <div className="text-[11px] text-muted-foreground italic">
        No rows landed yet — bronze refresh runs on its own cadence.
      </div>
    );
  }

  // Use the first row's keys as the schema. Insertion order in JSON
  // matches the SELECT projection, which is the natural reading order
  // for the user.
  const columns = Object.keys(rows[0]);

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-1">
        {columns.map((c) => (
          <Badge key={c} variant="outline" className="text-[10px] font-mono">
            {c}
          </Badge>
        ))}
      </div>
      <div className="overflow-x-auto rounded border border-border">
        <table className="w-full text-[11px] font-mono">
          <thead className="bg-muted/30">
            <tr>
              {columns.map((c) => (
                <th key={c} className="text-left px-2 py-1 font-medium text-muted-foreground whitespace-nowrap">
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className="border-t border-border">
                {columns.map((c) => (
                  <td key={c} className="px-2 py-1 whitespace-nowrap max-w-[200px] truncate" title={String(row[c] ?? "")}>
                    {row[c] === null || row[c] === undefined
                      ? <span className="text-muted-foreground">null</span>
                      : String(row[c])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="text-[10px] text-muted-foreground">
        Showing {rows.length} most recent {rows.length === 1 ? "row" : "rows"}.
      </div>
    </div>
  );
}


// Try-with-Zerobus inline panel.
//
// Renders a copy-pastable Python snippet that emits the same per-profile
// events to Delta via Databricks Zerobus (low-latency direct append).
// Backend handles snippet rendering — see /api/generate/demo-data/zerobus-snippet
// — so the per-profile generator code stays in one place
// (src/demo_streaming_schedule._PROFILE_GENERATORS_SOURCE).
//
// The panel defaults collapsed to keep the completion card tight; expand
// triggers a fetch with the current form values, and any form change
// re-fetches as long as it stays expanded.
function ZerobusSnippetPanel({
  profile, catalog, schema, table,
  eventsPerBatch, intervalSeconds,
}: {
  profile: string;
  catalog: string;
  schema: string;
  table: string;
  eventsPerBatch: number;
  intervalSeconds: number;
}) {
  const [open, setOpen] = useState(false);
  const [snippet, setSnippet] = useState<string>("");
  const [filenameSuggestion, setFilenameSuggestion] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    api.post<{ snippet: string; language: string; filename_suggestion: string }>(
      "/generate/demo-data/zerobus-snippet",
      {
        profile,
        catalog,
        schema,
        table,
        events_per_batch: eventsPerBatch,
        interval_seconds: intervalSeconds,
      },
    )
      .then((r) => {
        if (cancelled) return;
        setSnippet(r.snippet);
        setFilenameSuggestion(r.filename_suggestion);
      })
      .catch((e: any) => {
        if (cancelled) return;
        setError(e?.message ?? "snippet unavailable");
        setSnippet("");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [open, profile, catalog, schema, table, eventsPerBatch, intervalSeconds]);

  function downloadAsFile() {
    if (!snippet) return;
    const blob = new Blob([snippet], { type: "text/x-python" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filenameSuggestion || `zerobus_emit_${profile}.py`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  return (
    <div className="border border-border rounded-md bg-muted/20 p-3 space-y-2">
      <button
        type="button"
        className="w-full flex items-center justify-between gap-2 text-left"
        onClick={() => setOpen(o => !o)}
        aria-expanded={open}
      >
        <span className="flex items-center gap-2">
          <Radio className="h-3.5 w-3.5 text-[#E8453C]" />
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
            Try with Zerobus (low-latency direct append)
          </span>
          <Badge variant="outline" className="text-[10px]">Preview</Badge>
        </span>
        {open
          ? <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" />
          : <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
      </button>

      {open && (
        <div className="space-y-2">
          <p className="text-[11px] text-muted-foreground">
            Zerobus is Databricks' direct append API — no warehouse, no Volume,
            no Auto Loader. The snippet below uses the same per-profile event
            generator as this demo, configured for your current selections.
            The official <code className="text-[10px] bg-background px-1 rounded">databricks-zerobus</code> Python SDK
            is not yet released; the snippet's import line will start working
            once it ships.
          </p>

          {loading && (
            <div className="text-[11px] text-muted-foreground italic">
              Rendering snippet...
            </div>
          )}

          {error && (
            <div className="text-[11px] text-amber-600 flex items-center gap-2">
              <span>Snippet unavailable: {error}</span>
              <Button size="sm" variant="ghost" onClick={() => {
                setOpen(false);
                setTimeout(() => setOpen(true), 0);
              }}>
                Retry
              </Button>
            </div>
          )}

          {!loading && !error && snippet && (
            <>
              <div className="flex items-center justify-end gap-1">
                <Button size="sm" variant="ghost" onClick={() => copyToClipboard(snippet, "Snippet copied")}>
                  <ClipboardCopy className="h-3.5 w-3.5 mr-1" />Copy
                </Button>
                <Button size="sm" variant="ghost" onClick={downloadAsFile}>
                  <Download className="h-3.5 w-3.5 mr-1" />Download .py
                </Button>
              </div>
              <pre className="text-[11px] font-mono bg-background border border-border rounded p-2 overflow-x-auto whitespace-pre max-h-96">
                {snippet}
              </pre>
            </>
          )}
        </div>
      )}
    </div>
  );
}
