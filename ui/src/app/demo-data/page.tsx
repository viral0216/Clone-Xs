// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { useCurrency } from "@/hooks/useSettings";
import {
  Database, Loader2, CheckCircle2, XCircle, Play, RefreshCw, Clock,
  ChevronDown, ChevronUp, Info, Zap, DollarSign, Trash2, ExternalLink,
  ClipboardCopy, Check, Download,
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

  // Preview state
  const [previewOpen, setPreviewOpen] = useState(true);
  const [industryDetailOpen, setIndustryDetailOpen] = useState(false);
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
      };
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
        <CardContent className="space-y-5">
          {/* Catalog Name */}
          <div>
            <label className="text-sm font-medium">
              Catalog Name <span className="text-red-500">*</span>
            </label>
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
            <label className="text-sm font-medium">Industries</label>
            <div className="flex flex-wrap gap-3 mt-2">
              {INDUSTRIES.map((industry) => (
                <label
                  key={industry}
                  className="flex items-center gap-2 cursor-pointer select-none"
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
            <label className="text-sm font-medium">Scale Factor</label>
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

          {/* Owner */}
          <div>
            <label className="text-sm font-medium">Owner</label>
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
            <label className="text-sm font-medium">Storage Location</label>
            <Input
              value={storageLocation}
              onChange={(e) => setStorageLocation(e.target.value)}
              placeholder="abfss://container@storage.dfs.core.windows.net/path"
              className="mt-1 max-w-xl"
              disabled={isRunning}
            />
            <p className="text-xs text-muted-foreground mt-1">Optional. Custom managed storage location for the catalog.</p>
          </div>

          {/* Date Range */}
          <div className="grid grid-cols-2 gap-4 max-w-xl">
            <div>
              <label className="text-sm font-medium">Start Date</label>
              <Input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="mt-1"
                disabled={isRunning}
              />
            </div>
            <div>
              <label className="text-sm font-medium">End Date</label>
              <Input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="mt-1"
                disabled={isRunning}
              />
            </div>
          </div>

          {/* Destination Catalog */}
          <div>
            <label className="text-sm font-medium">Destination Catalog</label>
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
            <label className="flex items-center gap-2 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={dropExisting}
                onChange={(e) => setDropExisting(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-red-600 focus:ring-red-500"
              />
              <span className="text-sm font-medium">Drop Existing</span>
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              If checked, the existing catalog will be dropped and recreated.
            </p>
          </div>

          {/* Medallion Architecture */}
          <div>
            <label className="flex items-center gap-2 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={medallion}
                onChange={(e) => setMedallion(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Medallion Architecture (Bronze / Silver / Gold)</span>
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates bronze (raw), silver (cleaned), and gold (aggregated) schemas per industry.
            </p>
          </div>

          {/* UC Best Practices */}
          {medallion && (
            <div className="ml-6">
              <label className="flex items-center gap-2 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={ucBestPractices}
                  onChange={(e) => setUcBestPractices(e.target.checked)}
                  disabled={isRunning}
                  className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
                />
                <span className="text-sm font-medium">UC Best Practices Naming</span>
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
            <label className="flex items-center gap-2 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={createFunctions}
                onChange={(e) => setCreateFunctions(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Create UDFs (User-Defined Functions)</span>
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates 20 SQL UDFs per industry for masking, formatting, validation, and business logic.
            </p>
          </div>

          {/* Create Volumes */}
          <div>
            <label className="flex items-center gap-2 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={createVolumes}
                onChange={(e) => setCreateVolumes(e.target.checked)}
                disabled={isRunning}
                className="h-4 w-4 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
              />
              <span className="text-sm font-medium">Create Volumes with Sample Files</span>
            </label>
            <p className="text-xs text-muted-foreground mt-1 ml-6">
              Creates managed volumes and exports sample CSV files (1000 rows per table).
            </p>
          </div>

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
                      line.includes("ERROR") ? "text-red-400" :
                      line.includes("WARNING") ? "text-gray-400" :
                      line.includes("done") || line.includes("Created") || line.includes("created") ? "text-gray-300" :
                      line.includes("Creating") || line.includes("Generating") ? "text-[#E8453C]" : ""
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
          </CardContent>
        </Card>
      )}
    </div>
  );
}
