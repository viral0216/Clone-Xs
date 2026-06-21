// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { Link, useNavigate } from "react-router-dom";
import { ExternalLink, Loader2, RefreshCw, AlertCircle, TreePine, GitBranch, Network, Layers, Search as SearchIcon } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";

const VIEWS = [
  { key: "tree",     label: "Tree",           icon: TreePine,  href: "/assessment/inventory/tree",     apiView: "tree" },
  { key: "sunburst", label: "Sunburst",        icon: GitBranch, href: "/assessment/inventory/sunburst", apiView: "sunburst" },
  { key: "hubspoke", label: "Hub & Spoke",     icon: Network,   href: "/assessment/inventory/hubspoke", apiView: "hubspoke" },
  { key: "topology", label: "Infrastructure",  icon: Layers,    href: "/assessment/inventory/topology", apiView: "topology" },
];

interface Props {
  viewKey: string;
  title: string;
  description: string;
}

export default function IframeView({ viewKey, title, description }: Props) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const navigate = useNavigate();

  const [loaded, setLoaded] = useState(false);
  const [loadError, setLoadError] = useState(false);
  const [stats, setStats] = useState<any>(null);
  const [reload, setReload] = useState(0);

  const view = VIEWS.find(v => v.key === viewKey)!;
  const src = `/api/assessment/html/${view.apiView}`;

  // Fetch stats strip
  useEffect(() => {
    api.get("/assessment/inventory").then(d => setStats(d?.stats ? d : null)).catch(() => {});
  }, []);

  // Keyboard shortcut: F = open fullscreen
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (e.key === "f" || e.key === "F") window.open(src, "_blank");
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [src]);

  function handleLoad() {
    try {
      const doc = iframeRef.current?.contentDocument;
      const body = doc?.body?.innerText?.trim() ?? "";
      // FastAPI 404/500 responses are JSON with "detail" key; no real HTML title
      if (!doc?.title && (body.startsWith('{"detail"') || (body.length > 0 && body.length < 300 && body.includes('"detail"')))) {
        setLoadError(true);
      }
    } catch {}
    setLoaded(true);
  }

  function handleReload() {
    setLoaded(false);
    setLoadError(false);
    setReload(r => r + 1);
  }

  const st = stats?.stats;
  const scannedAt = stats?.scanned_at ? new Date(stats.scanned_at) : null;
  const ago = scannedAt ? (() => {
    const diff = Date.now() - scannedAt.getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
  })() : null;

  const Icon = view.icon;

  return (
    <div className="flex flex-col h-full space-y-2">
      <PageHeader
        title={title}
        icon={Icon}
        breadcrumbs={["Assessment", "UC Inventory", title]}
        description={description}
        actions={
          <div className="flex items-center gap-2">
            <Link to="/assessment/inventory/search">
              <Button size="sm" variant="outline">
                <SearchIcon className="h-4 w-4 mr-1.5" />
                Search Objects
              </Button>
            </Link>
            <Button size="sm" variant="outline" onClick={handleReload} title="Reload view">
              <RefreshCw className="h-4 w-4" />
            </Button>
            <Button size="sm" variant="outline" onClick={() => window.open(src, "_blank")} title="Open fullscreen (F)">
              <ExternalLink className="h-4 w-4 mr-1.5" />
              Fullscreen
            </Button>
          </div>
        }
      />

      {/* Stats strip */}
      {st && (
        <div className="flex items-center gap-4 px-1 py-1.5 text-xs text-muted-foreground flex-wrap">
          {[
            ["Catalogs", st.catalogs],
            ["Schemas", st.schemas],
            ["Tables", st.tables],
            ["Volumes", st.volumes],
            ["Functions", st.functions],
            ["Models", st.registered_models],
          ].filter(([, v]) => v).map(([label, value]) => (
            <span key={label as string} className="flex items-center gap-1">
              <span className="font-semibold text-foreground">{Number(value).toLocaleString()}</span>
              {label}
            </span>
          ))}
          {ago && <span className="ml-auto text-[11px]">Last scanned {ago}</span>}
          <span className="text-[11px] text-muted-foreground/60 hidden sm:inline">Press <kbd className="px-1 rounded border border-border font-mono">F</kbd> for fullscreen</span>
        </div>
      )}

      {/* Cross-view tabs */}
      <div className="flex items-center gap-1 border-b border-border pb-0 -mb-2">
        {VIEWS.map(v => {
          const VIcon = v.icon;
          const active = v.key === viewKey;
          return (
            <Link key={v.key} to={v.href}>
              <button
                className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium rounded-t-md transition-colors border-b-2 -mb-px ${
                  active
                    ? "text-foreground border-primary bg-background"
                    : "text-muted-foreground border-transparent hover:text-foreground hover:bg-muted/40"
                }`}
              >
                <VIcon className="h-3.5 w-3.5" />
                {v.label}
              </button>
            </Link>
          );
        })}
      </div>

      {/* Iframe area */}
      <div className="relative flex-1 rounded-b-lg rounded-tr-lg border border-border overflow-hidden" style={{ minHeight: "calc(100vh - 220px)" }}>
        {!loaded && !loadError && (
          <div className="absolute inset-0 flex items-center justify-center bg-background z-10">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <span className="ml-2 text-sm text-muted-foreground">Loading {title.toLowerCase()}…</span>
          </div>
        )}

        {loadError ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-4 bg-background z-10 px-4">
            <AlertCircle className="h-10 w-10 text-muted-foreground/40" />
            <div className="text-center">
              <p className="font-medium text-sm">No visualization available</p>
              <p className="text-xs text-muted-foreground mt-1 max-w-[320px]">
                The {title.toLowerCase()} requires an inventory scan with UC Inventory enabled. Run a scan to generate this view.
              </p>
            </div>
            <div className="flex gap-2">
              <Button size="sm" variant="outline" onClick={handleReload}>
                <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
                Retry
              </Button>
              <Button size="sm" onClick={() => navigate("/assessment/run")}>
                Run Scan
              </Button>
            </div>
          </div>
        ) : (
          <iframe
            key={reload}
            ref={iframeRef}
            src={src}
            className="w-full h-full border-0"
            title={title}
            onLoad={handleLoad}
            style={{ minHeight: "calc(100vh - 220px)" }}
          />
        )}
      </div>
    </div>
  );
}
