// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
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
import { useStreamingEmit, useStreamingStop, useVolumes } from "@/hooks/useApi";
import {
  Database, Loader2, CheckCircle2, XCircle, Play, RefreshCw, Clock,
  ChevronDown, ChevronUp, Info, Zap, DollarSign, Trash2, ExternalLink,
  ClipboardCopy, Check, Download, Radio, StopCircle,
} from "lucide-react";

const INDUSTRIES = ["healthcare", "financial", "retail", "telecom", "manufacturing", "energy", "education", "real_estate", "logistics", "insurance"] as const;

const SCALE_OPTIONS = [
  { value: "0.01", label: "0.01 — Test (~10M rows)" },
  { value: "0.1", label: "0.1 — Small (~100M rows)" },
  { value: "0.5", label: "0.5 — Medium (~500M rows)" },
  { value: "1.0", label: "1.0 — Full (~1B rows)" },
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

  // Job state
  const [jobId, setJobId] = useState<string | null>(null);
  const [job, setJob] = useState<any>(null);
  const [submitting, setSubmitting] = useState(false);
  const pollRef = useRef<NodeJS.Timeout | null>(null);
  const logsEndRef = useRef<HTMLDivElement | null>(null);

  // Top-level "which generator?" switch. The page houses two distinct
  // generators (batch catalog + streaming events); each gets its own
  // tab so users aren't scrolling past inapplicable controls.
  // Persisted to sessionStorage so refresh keeps the user where they
  // were.
  const [activeGenTab, setActiveGenTab] = useState<"batch" | "streaming">(() => {
    try { return (sessionStorage.getItem("clxs-demo-gen-tab") as "batch" | "streaming") || "batch"; }
    catch { return "batch"; }
  });

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
  const [streamProfile, setStreamProfile] = useState<"generic_sensor" | "industrial_machine" | "car_obd2">("generic_sensor");
  const [streamEventsPerBatch, setStreamEventsPerBatch] = useState(100);
  const [streamIntervalSeconds, setStreamIntervalSeconds] = useState(5);
  const [streamDurationSeconds, setStreamDurationSeconds] = useState(60);
  // Destination mode for streaming events:
  //   "volume"        — JSON files only, no Bronze
  //   "volume_bronze" — files + auto-create Bronze STREAMING TABLE (default)
  //   "direct_table"  — INSERT INTO Delta table directly (no Volume)
  const [streamDestination, setStreamDestination] = useState<"volume" | "volume_bronze" | "direct_table">("volume_bronze");
  const [streamBronzeTable, setStreamBronzeTable] = useState("");
  // Legacy auto-create flag — derived from destination on submit. Kept
  // as state only to render the refresh-cadence input in volume_bronze mode.
  const [streamBronzeRefreshMinutes, setStreamBronzeRefreshMinutes] = useState(5);
  const [streamingJobId, setStreamingJobId] = useState<string | null>(null);
  const [streamingJob, setStreamingJob] = useState<any>(null);
  const streamingPollRef = useRef<NodeJS.Timeout | null>(null);
  // Per-tick throughput series captured from polling. Each entry is one
  // emission tick; deduped by tick number so faster polling than the
  // emit interval doesn't double-record.
  const [streamingSeries, setStreamingSeries] = useState<Array<{
    tick: number;
    elapsed: number;
    events: number;     // cumulative events emitted up to this tick
    delta: number;      // events in this tick (cumulative diff)
  }>>([]);
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
    setJob(null);
    setJobId(null);

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

      const res = await api.post("/generate/demo-data", body);
      if (res.job_id) {
        setJobId(res.job_id);
        toast.success(`Demo data generation submitted (Job ${res.job_id})`);
      } else {
        toast.error("Unexpected response — no job_id returned");
      }
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  };

  // Poll for job status
  useEffect(() => {
    if (!jobId) return;

    const poll = async () => {
      try {
        const data = await api.get(`/clone/${jobId}`);
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          if (pollRef.current) clearInterval(pollRef.current);
          if (data.status === "completed") {
            toast.success("Demo data generated successfully");
          } else {
            toast.error(data.error || "Demo data generation failed");
          }
        }
      } catch {
        // Silently retry on poll errors
      }
    };

    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [jobId]);

  // Poll the streaming job — separate effect so the two job lifecycles
  // don't interfere. Uses the same /clone/{id} endpoint since the
  // JobManager surfaces all job types through it.
  useEffect(() => {
    if (!streamingJobId) return;
    const poll = async () => {
      try {
        const data = await api.get(`/clone/${streamingJobId}`);
        setStreamingJob(data);
        // Capture this tick into the throughput series — dedupe by
        // tick number so we don't get duplicate samples when the
        // 2s poll fires faster than the emit interval.
        const prog = data?.progress;
        if (prog && typeof prog.ticks === "number" && typeof prog.events_emitted === "number") {
          setStreamingSeries((prev) => {
            if (prev.length && prev[prev.length - 1].tick === prog.ticks) return prev;
            const lastEvents = prev.length ? prev[prev.length - 1].events : 0;
            return [...prev, {
              tick: prog.ticks,
              elapsed: typeof prog.elapsed_seconds === "number" ? prog.elapsed_seconds : 0,
              events: prog.events_emitted,
              delta: prog.events_emitted - lastEvents,
            }];
          });
        }
        if (data.status === "completed" || data.status === "failed") {
          if (streamingPollRef.current) clearInterval(streamingPollRef.current);
          if (data.status === "completed") {
            toast.success("Streaming emission completed");
          } else {
            toast.error(data.error || "Streaming emission failed");
          }
        }
      } catch {
        // ignore transient errors
      }
    };
    poll();
    streamingPollRef.current = setInterval(poll, 2000);
    return () => {
      if (streamingPollRef.current) clearInterval(streamingPollRef.current);
    };
  }, [streamingJobId]);

  const handleStartStreaming = async () => {
    if (!streamCatalog.trim() || !streamSchema.trim()) {
      toast.error("Catalog and schema are required");
      return;
    }
    try {
      setStreamingJob(null);
      setStreamingJobId(null);
      // Reset chart series on a new run so we don't merge runs visually.
      setStreamingSeries([]);
      const res = await streamingEmit.mutateAsync({
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
      });
      if (res?.job_id) {
        setStreamingJobId(res.job_id);
        toast.success(`Streaming emission started (Job ${res.job_id})`);
      }
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
    setJobId(null);
    setJob(null);
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
          <div>
            <FieldLabel hint="Name of the new demo catalog. Must not already exist unless 'Drop Existing' is checked.">
              Catalog Name <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              value={catalogName}
              onChange={(e) => setCatalogName(e.target.value)}
              placeholder="demo_catalog"
              className="mt-1 max-w-md"
              disabled={isRunning}
            />
          </div>

          {/* Industries */}
          <div>
            <FieldLabel hint="Each industry generates a domain-specific schema (e.g. healthcare gets patients, encounters, claims). Pick one for a quick demo, several for cross-domain analytics scenarios.">Industries</FieldLabel>
            <div className="flex flex-wrap gap-3 mt-2">
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
          <div>
            <FieldLabel hint="Multiplier on row counts. 0.01 = ~10M rows total (good for laptop demos); 1.0 = ~1B rows (production-scale benchmark).">Scale Factor</FieldLabel>
            <select
              value={scaleFactor}
              onChange={(e) => setScaleFactor(e.target.value)}
              disabled={isRunning}
              className="flex h-9 w-full max-w-md rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring mt-1"
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
            <div>
              <FieldLabel hint="Earliest date for generated transactional data (orders, claims, events).">Start Date</FieldLabel>
              <Input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="mt-1"
                disabled={isRunning}
              />
            </div>
            <div>
              <FieldLabel hint="Latest date for generated transactional data. Window between start and end determines volume per day.">End Date</FieldLabel>
              <Input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="mt-1"
                disabled={isRunning}
              />
            </div>
          </div>
            </TabsContent>

            <TabsContent value="catalog" className="space-y-5 mt-4">
          {/* Owner */}
          <div>
            <FieldLabel hint="Sets the catalog owner principal — usually a team email or group SCIM name. Defaults to the current user.">Owner</FieldLabel>
            <Input
              value={owner}
              onChange={(e) => setOwner(e.target.value)}
              placeholder="team-name or user@domain.com"
              className="mt-1 max-w-md"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground mt-1">Optional. Sets the catalog owner.</p>
          </div>

          {/* Storage Location */}
          <div>
            <FieldLabel hint="External storage URI for managed tables. Required if the workspace doesn't have a default Unity Catalog storage root configured.">Storage Location</FieldLabel>
            <Input
              value={storageLocation}
              onChange={(e) => setStorageLocation(e.target.value)}
              placeholder="abfss://container@storage.dfs.core.windows.net/path"
              className="mt-1 max-w-xl"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground mt-1">Optional. Custom managed storage location for the catalog.</p>
          </div>

          {/* Destination Catalog */}
          <div>
            <FieldLabel hint="If set, the generated catalog is auto-cloned to this destination after generation completes.">Destination Catalog</FieldLabel>
            <Input
              value={destCatalog}
              onChange={(e) => setDestCatalog(e.target.value)}
              placeholder="e.g. prod_catalog"
              className="mt-1 max-w-xl"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground mt-1">Optional. When filled, the generated catalog will be automatically cloned to this destination.</p>
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
                {isComplete && <CheckCircle2 className="h-5 w-5 text-foreground" />}
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
                        await api.post(`/clone/${jobId}/cancel`);
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
          <CardContent className="space-y-4">
            {/* Destination mode — controls which downstream fields are visible
                and what the runner does each tick. */}
            <div className="border border-dashed border-border rounded-md p-3 bg-muted/20">
              <FieldLabel hint="Volume only: emit JSON files; you wire Auto Loader yourself. Volume + Bronze: same files plus an auto-created STREAMING TABLE on a CRON refresh (needs DBSQL Serverless tier that supports it). Direct to table: each tick INSERTs straight into a Delta table — no Volume, no Auto Loader, works on any tier including Free Edition.">
                Destination
              </FieldLabel>
              <div className="mt-2 grid grid-cols-1 md:grid-cols-3 gap-2">
                {[
                  { val: "volume", title: "Volume only", sub: "JSON files → Volume" },
                  { val: "volume_bronze", title: "Volume + Bronze", sub: "Files + Auto Loader STREAMING TABLE" },
                  { val: "direct_table", title: "Direct to table", sub: "INSERT each batch into Delta (no Volume)" },
                ].map((opt) => (
                  <label
                    key={opt.val}
                    className={`flex items-start gap-2 p-2 border rounded-md cursor-pointer text-xs transition-colors ${
                      streamDestination === opt.val
                        ? "border-[#E8453C] bg-[#E8453C]/5"
                        : "border-input hover:bg-muted/30"
                    }`}
                  >
                    <input
                      type="radio"
                      name="stream-destination"
                      value={opt.val}
                      checked={streamDestination === opt.val}
                      onChange={() => setStreamDestination(opt.val as typeof streamDestination)}
                      className="mt-0.5 h-3.5 w-3.5 text-[#E8453C] focus:ring-[#E8453C]"
                    />
                    <div>
                      <div className="font-medium">{opt.title}</div>
                      <div className="text-[10px] text-muted-foreground">{opt.sub}</div>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {/* Device profile */}
            <div>
              <FieldLabel>Device profile</FieldLabel>
              <select className="w-full md:w-auto h-9 px-2 text-sm bg-background border border-input rounded-md mt-1"
                value={streamProfile}
                onChange={(e) => setStreamProfile(e.target.value as typeof streamProfile)}>
                <option value="generic_sensor">Generic IoT Sensor</option>
                <option value="industrial_machine">Industrial Machine</option>
                <option value="car_obd2">Car OBD-II</option>
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
                  <label className="text-xs text-muted-foreground">Catalog</label>
                  <select
                    className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md mt-1"
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
                  <label className="text-xs text-muted-foreground">Schema</label>
                  <select
                    className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md mt-1"
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
                      <label className="text-xs text-muted-foreground">Bronze table</label>
                      <Input
                        value={streamBronzeTable}
                        onChange={(e) => setStreamBronzeTable(e.target.value)}
                        placeholder={`bronze_${streamProfile}`}
                        className="mt-1"
                      />
                      <p className="text-[10px] text-muted-foreground mt-1">
                        Delta table created in the chosen schema. Empty → defaults to <code className="text-[10px] bg-muted/50 px-1 rounded">bronze_{streamProfile}</code>. Each tick INSERTs one batch directly.
                      </p>
                    </>
                  ) : (
                    <>
                      <label className="text-xs text-muted-foreground">Volume</label>
                      {(() => {
                        const matches = (volumesQuery.data || [])
                          .filter((v) => (!streamCatalog || v.catalog === streamCatalog)
                                      && (!streamSchema || v.schema === streamSchema))
                          .map((v) => v.name);
                        const uniqueExisting = Array.from(new Set(matches));
                        return (
                          <>
                            <select
                              className="w-full h-9 px-2 text-sm bg-background border border-input rounded-md mt-1"
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
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div>
                <FieldLabel>Events per batch</FieldLabel>
                <Input type="number" min={1} max={10000} value={streamEventsPerBatch}
                  onChange={(e) => setStreamEventsPerBatch(Math.max(1, Math.min(10000, parseInt(e.target.value) || 100)))}
                  className="mt-1" />
              </div>
              <div>
                <FieldLabel>Interval (seconds)</FieldLabel>
                <Input type="number" min={1} max={300} value={streamIntervalSeconds}
                  onChange={(e) => setStreamIntervalSeconds(Math.max(1, Math.min(300, parseInt(e.target.value) || 5)))}
                  className="mt-1" />
              </div>
              <div>
                <FieldLabel>Total duration (seconds, max 3600)</FieldLabel>
                <Input type="number" min={1} max={3600} value={streamDurationSeconds}
                  onChange={(e) => setStreamDurationSeconds(Math.max(1, Math.min(3600, parseInt(e.target.value) || 60)))}
                  className="mt-1" />
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
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Refresh every</span>
                  <Input type="number" min={1} max={60} value={streamBronzeRefreshMinutes}
                    onChange={(e) => setStreamBronzeRefreshMinutes(Math.max(1, Math.min(60, parseInt(e.target.value) || 5)))}
                    className="w-20 h-7 text-xs" />
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
            </div>

            {/* Live progress */}
            {streamingJob && (
              <Card className="bg-card border-border">
                <CardContent className="pt-4 space-y-2">
                  <div className="flex items-center gap-2">
                    {streamingJob.status === "completed" ? <CheckCircle2 className="h-4 w-4 text-[#E8453C]" />
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
                  {streamingJob.progress && (
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
                      <div><span className="text-muted-foreground">Events emitted:</span> <span className="font-mono">{streamingJob.progress.events_emitted ?? 0}</span></div>
                      <div><span className="text-muted-foreground">Files written:</span> <span className="font-mono">{streamingJob.progress.files_written ?? 0}</span></div>
                      <div><span className="text-muted-foreground">Rows inserted:</span> <span className="font-mono">{streamingJob.progress.rows_inserted ?? 0}</span></div>
                      <div><span className="text-muted-foreground">Ticks:</span> <span className="font-mono">{streamingJob.progress.ticks ?? 0}</span></div>
                      <div><span className="text-muted-foreground">Elapsed:</span> <span className="font-mono">{streamingJob.progress.elapsed_seconds ?? 0}s</span></div>
                    </div>
                  )}

                  {/* Throughput chart — dual-axis line chart of cumulative
                      events (left) and per-tick delta (right) over elapsed
                      seconds. Hidden until we have ≥2 samples (one point
                      isn't a line). */}
                  {streamingSeries.length >= 2 && (
                    <div className="border border-border rounded-md bg-background p-2 mt-2">
                      <div className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide mb-1">Throughput</div>
                      <ResponsiveContainer width="100%" height={160}>
                        <LineChart data={streamingSeries} margin={{ top: 6, right: 6, bottom: 0, left: 0 }}>
                          <CartesianGrid strokeDasharray="3 3" className="opacity-20" />
                          <XAxis
                            dataKey="elapsed"
                            type="number"
                            domain={[0, "dataMax"]}
                            tick={{ fontSize: 10 }}
                            stroke="var(--text-muted, #666)"
                            label={{ value: "elapsed (s)", position: "insideBottom", offset: -2, style: { fontSize: 10, fill: "var(--text-muted, #666)" } }}
                          />
                          <YAxis
                            yAxisId="cumulative"
                            tick={{ fontSize: 10 }}
                            stroke="var(--text-muted, #666)"
                            allowDecimals={false}
                          />
                          <YAxis
                            yAxisId="delta"
                            orientation="right"
                            tick={{ fontSize: 10 }}
                            stroke="var(--text-muted, #666)"
                            allowDecimals={false}
                          />
                          <Tooltip
                            contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 11 }}
                            formatter={(v: number, name: string) => [v, name === "events" ? "Cumulative events" : "Events / tick"]}
                            labelFormatter={(v: number) => `t=${v}s`}
                          />
                          <Legend wrapperStyle={{ fontSize: 10 }} />
                          <Line
                            yAxisId="cumulative"
                            type="monotone"
                            dataKey="events"
                            name="Cumulative events"
                            stroke="#E8453C"
                            strokeWidth={2}
                            dot={false}
                            isAnimationActive={false}
                          />
                          <Line
                            yAxisId="delta"
                            type="monotone"
                            dataKey="delta"
                            name="Events / tick"
                            stroke="#374151"
                            strokeWidth={1.5}
                            strokeDasharray="3 3"
                            dot={false}
                            isAnimationActive={false}
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                  {streamingJob.progress?.current_batch_path && (
                    <div className="text-[11px] text-muted-foreground font-mono truncate" title={streamingJob.progress.current_batch_path}>
                      Latest: {streamingJob.progress.current_batch_path}
                    </div>
                  )}
                  {/* Bronze status — only shown when result has landed */}
                  {streamingJob.result?.bronze_status === "created" && streamingJob.result?.bronze_table_fqn && (
                    <div className="border-t border-border pt-2 mt-2 text-xs">
                      <CheckCircle2 className="h-3.5 w-3.5 text-[#E8453C] inline mr-1" />
                      Bronze streaming table created: <code className="text-[11px] bg-muted/50 px-1 rounded">{streamingJob.result.bronze_table_fqn}</code>
                      <a href={`/preview?catalog=${streamCatalog}&schema=${streamSchema}&table=bronze_${streamProfile}`}
                        className="text-[#E8453C] hover:underline ml-2">
                        Query latest rows →
                      </a>
                    </div>
                  )}
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
          </CardContent>
      </Card>
      </>)}
    </div>
  );
}
