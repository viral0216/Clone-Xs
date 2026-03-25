// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useNavigate } from "react-router-dom";
import PageHeader from "@/components/PageHeader";
import {
  LayoutTemplate, ArrowRight, Copy, Shield, Zap, RefreshCw,
  Database, GitCompareArrows, HardDrive, Eye, Lock, Server,
  CheckCircle, XCircle, Layers, ChevronDown, ChevronUp,
} from "lucide-react";

interface Template {
  key?: string;
  name: string;
  description: string;
  long_description?: string;
  clone_type?: string;
  validate?: boolean;
  dry_run?: boolean;
  parallel?: boolean;
  config?: Record<string, any>;
  settings?: Record<string, unknown>;
}

const TEMPLATE_ICONS: Record<string, any> = {
  "dev-copy": Shield,
  "dr-backup": HardDrive,
  "test-refresh": RefreshCw,
  "staging-promote": ArrowRight,
  "incremental-sync": GitCompareArrows,
  "schema-only": Layers,
  "cross-workspace": Server,
  "dev-refresh": Zap,
  "dr-replica": Database,
  "audit-copy": Eye,
  "pii-safe": Lock,
  "minimal": Zap,
  "full-mirror": Copy,
};

const TEMPLATE_COLORS: Record<string, string> = {
  "dev-copy": "text-muted-foreground bg-muted/30",
  "dr-backup": "text-red-500 bg-red-500/10",
  "test-refresh": "text-muted-foreground bg-muted/30",
  "staging-promote": "text-foreground bg-muted/30",
  "incremental-sync": "text-[#E8453C] bg-[#E8453C]/10",
  "schema-only": "text-gray-500 bg-gray-500/10",
  "cross-workspace": "text-muted-foreground bg-muted/30",
  "dev-refresh": "text-muted-foreground bg-muted/30",
  "dr-replica": "text-red-400 bg-red-400/10",
  "audit-copy": "text-muted-foreground bg-muted/30",
  "pii-safe": "text-muted-foreground bg-muted/30",
  "minimal": "text-foreground bg-muted/30",
  "full-mirror": "text-[#E8453C] bg-[#E8453C]/10",
};

const CATEGORIES = [
  { label: "All", value: "all" },
  { label: "Development", value: "dev" },
  { label: "Production", value: "prod" },
  { label: "Disaster Recovery", value: "dr" },
  { label: "Security", value: "security" },
];

function getCategory(key: string): string[] {
  if (["dev-copy", "dev-refresh", "test-refresh", "minimal", "schema-only"].includes(key)) return ["dev"];
  if (["staging-promote", "full-mirror", "incremental-sync"].includes(key)) return ["prod"];
  if (["dr-backup", "dr-replica", "cross-workspace"].includes(key)) return ["dr"];
  if (["pii-safe", "audit-copy"].includes(key)) return ["security"];
  // Fallback: try to infer from name/key
  if (key.includes("dev") || key.includes("test") || key.includes("minimal")) return ["dev"];
  if (key.includes("dr") || key.includes("disaster") || key.includes("replica")) return ["dr"];
  if (key.includes("pii") || key.includes("audit") || key.includes("compliance")) return ["security"];
  if (key.includes("prod") || key.includes("staging") || key.includes("mirror")) return ["prod"];
  return ["dev"];
}

function ConfigBadge({ label, enabled }: { label: string; enabled: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded ${
      enabled
        ? "bg-muted/40 text-foreground"
        : "bg-muted text-muted-foreground"
    }`}>
      {enabled ? <CheckCircle className="h-2.5 w-2.5" /> : <XCircle className="h-2.5 w-2.5 opacity-40" />}
      {label}
    </span>
  );
}

export default function TemplatesPage() {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("all");
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    api.get<Template[]>("/templates")
      .then((res) => setTemplates(Array.isArray(res) && res.length > 0 ? res : []))
      .catch(() => setTemplates([]))
      .finally(() => setLoading(false));
  }, []);

  const useTemplate = (t: Template) => {
    const params = new URLSearchParams();
    const cfg = t.config || t.settings || {};
    if (t.key) params.set("template", t.key);
    // Pass all config values as URL params so clone page can read them
    for (const [k, v] of Object.entries(cfg)) {
      if (v === null || v === undefined) continue;
      // Skip complex objects (masking_rules, clone_policies)
      if (typeof v === "object") continue;
      params.set(k, String(v));
    }
    navigate(`/clone?${params.toString()}`);
  };

  const filtered = filter === "all"
    ? templates
    : templates.filter((t) => getCategory(t.key || "").includes(filter));

  return (
    <div className="space-y-4">
      <PageHeader
        title="Clone Templates"
        description={`${templates.length} pre-built clone recipes — Production Mirror, Dev Sandbox, DR Copy, Compliance Snapshot, and more. Each template pre-fills optimal settings for its use case.`}
        icon={LayoutTemplate}
        breadcrumbs={["Operations", "Templates"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-create-table-clone"
        docsLabel="Clone best practices"
      />

      {/* Category Filter */}
      <div className="flex gap-2">
        {CATEGORIES.map((cat) => (
          <button
            key={cat.value}
            onClick={() => setFilter(cat.value)}
            className={`px-3 py-1.5 text-xs font-medium rounded-full transition-all ${
              filter === cat.value
                ? "bg-[#E8453C] text-white"
                : "bg-muted text-muted-foreground hover:bg-muted/80"
            }`}
          >
            {cat.label}
          </button>
        ))}
      </div>

      {/* Templates Grid */}
      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <Card key={i} className="bg-card border-border animate-pulse">
              <CardContent className="pt-6 pb-5 space-y-4">
                <div className="h-10 w-10 bg-muted rounded-lg" />
                <div className="h-4 w-2/3 bg-muted rounded" />
                <div className="h-3 w-full bg-muted rounded" />
                <div className="h-8 w-full bg-muted rounded" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-16 text-muted-foreground">
          <LayoutTemplate className="h-12 w-12 mx-auto mb-3 opacity-30" />
          <p className="text-sm">No templates in this category</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map((t) => {
            const key = t.key || t.name.toLowerCase().replace(/\s+/g, "-");
            const Icon = TEMPLATE_ICONS[key] || Copy;
            const color = TEMPLATE_COLORS[key] || "text-[#E8453C] bg-[#E8453C]/10";
            const cfg = t.config || t.settings || {};
            const cloneType = cfg.clone_type || t.clone_type || "DEEP";

            return (
              <Card
                key={key}
                className="group bg-card border-border hover:border-[#E8453C]/40 hover:shadow-lg hover:shadow-[#E8453C]/5 transition-all duration-200 cursor-pointer"
                onClick={() => useTemplate(t)}
              >
                <CardContent className="pt-6 pb-5 space-y-4">
                  {/* Icon + Type badge */}
                  <div className="flex items-start justify-between">
                    <div className={`p-2.5 rounded-lg ${color}`}>
                      <Icon className="h-5 w-5" />
                    </div>
                    <Badge
                      variant="outline"
                      className={`text-[10px] font-bold ${
                        cloneType === "DEEP"
                          ? "border-[#E8453C]/30 text-[#E8453C] bg-[#E8453C]/5"
                          : "border-[#6B7280]/30 text-[#6B7280] bg-[#6B7280]/5"
                      }`}
                    >
                      {cloneType}
                    </Badge>
                  </div>

                  {/* Name + Description */}
                  <div>
                    <h2 className="font-semibold text-foreground group-hover:text-[#E8453C] transition-colors" style={{ fontSize: '16px' }}>
                      {t.name}
                    </h2>
                    <p className="text-xs text-muted-foreground mt-1 leading-relaxed">
                      {t.description}
                    </p>
                    {t.long_description && (
                      <>
                        <button
                          className="text-[11px] text-[#E8453C] hover:text-[#D93025] mt-1.5 flex items-center gap-0.5"
                          onClick={(e) => {
                            e.stopPropagation();
                            setExpandedKey(expandedKey === key ? null : key);
                          }}
                        >
                          {expandedKey === key ? (
                            <>Less <ChevronUp className="h-3 w-3" /></>
                          ) : (
                            <>More details <ChevronDown className="h-3 w-3" /></>
                          )}
                        </button>
                        {expandedKey === key && (
                          <p className="text-xs text-muted-foreground mt-2 leading-relaxed border-t border-border pt-2">
                            {t.long_description}
                          </p>
                        )}
                      </>
                    )}
                  </div>

                  {/* Config badges */}
                  <div className="flex flex-wrap gap-1.5">
                    <ConfigBadge label="Permissions" enabled={!!cfg.copy_permissions} />
                    <ConfigBadge label="Validate" enabled={!!cfg.validate_after_clone || !!t.validate} />
                    <ConfigBadge label="Rollback" enabled={!!cfg.enable_rollback} />
                    <ConfigBadge label="Checksum" enabled={!!cfg.validate_checksum} />
                    {cfg.masking_rules && (
                      <span className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded bg-muted/40 text-muted-foreground">
                        <Lock className="h-2.5 w-2.5" />
                        PII Masking ({cfg.masking_rules.length} rules)
                      </span>
                    )}
                  </div>

                  {/* Use button */}
                  <Button
                    size="sm"
                    variant="outline"
                    className="w-full group-hover:bg-[#E8453C] group-hover:text-white group-hover:border-[#E8453C] transition-all"
                    onClick={(e) => { e.stopPropagation(); useTemplate(t); }}
                  >
                    Use Template
                    <ArrowRight className="h-3.5 w-3.5 ml-1.5 group-hover:translate-x-0.5 transition-transform" />
                  </Button>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
