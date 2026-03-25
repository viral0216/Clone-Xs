// @ts-nocheck
import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Loader2, ClipboardCopy, Check, Download, ChevronDown, ChevronUp, ArrowDownToLine } from "lucide-react";

function logColor(line: string) {
  if (/error|ERROR|FAILED|failed/i.test(line)) return "text-red-400";
  if (/warn|WARNING/i.test(line)) return "text-yellow-400";
  if (/OK|success|cloned|completed|matched|created|done/i.test(line)) return "text-green-400";
  if (/progress|running|scanning|cloning|syncing|generating/i.test(line)) return "text-blue-400";
  return "text-gray-300";
}

interface LogPanelProps {
  logs: string[];
  jobId?: string;
  isRunning?: boolean;
  title?: string;
  maxHeight?: string;
  collapsible?: boolean;
  defaultExpanded?: boolean;
}

export default function LogPanel({
  logs,
  jobId = "",
  isRunning = false,
  title = "Logs",
  maxHeight = "max-h-72",
  collapsible = true,
  defaultExpanded = false,
}: LogPanelProps) {
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [filter, setFilter] = useState<"all" | "errors" | "warnings">("all");
  const [autoScroll, setAutoScroll] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs.length, autoScroll]);

  const logText = logs.join("\n");

  const handleCopy = async () => {
    await navigator.clipboard.writeText(logText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const blob = new Blob([logText], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${jobId || "logs"}-${new Date().toISOString().slice(0, 10)}.log`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const filteredLogs = logs.filter((line) => {
    if (filter === "all") return true;
    if (filter === "errors") return line.includes("ERROR");
    if (filter === "warnings") return line.includes("WARNING") || line.includes("ERROR");
    return true;
  });

  const errorCount = logs.filter((l) => l.includes("ERROR")).length;
  const warnCount = logs.filter((l) => l.includes("WARNING")).length;

  const heightClass = expanded ? "max-h-[600px]" : maxHeight;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center justify-between">
          <span className="flex items-center gap-2">
            {isRunning && <Loader2 className="h-3 w-3 animate-spin" />}
            {title}
          </span>
          <div className="flex items-center gap-1.5">
            {errorCount > 0 && (
              <button type="button" onClick={() => setFilter(filter === "errors" ? "all" : "errors")} aria-pressed={filter === "errors"} aria-label={`Filter ${errorCount} errors`}>
                <Badge
                  variant={filter === "errors" ? "default" : "outline"}
                  className="text-[10px] cursor-pointer text-red-500 border-red-500/30 px-1.5"
                >
                  {errorCount} errors
                </Badge>
              </button>
            )}
            {warnCount > 0 && (
              <button type="button" onClick={() => setFilter(filter === "warnings" ? "all" : "warnings")} aria-pressed={filter === "warnings"} aria-label={`Filter ${warnCount} warnings`}>
                <Badge
                  variant={filter === "warnings" ? "default" : "outline"}
                  className="text-[10px] cursor-pointer text-yellow-500 border-yellow-500/30 px-1.5"
                >
                  {warnCount} warn
                </Badge>
              </button>
            )}
            <Badge variant="outline" className="text-[10px] px-1.5">{logs.length} lines</Badge>
            {/* Auto-scroll toggle */}
            <Button
              variant="ghost"
              size="sm"
              className={`h-6 px-1.5 ${autoScroll ? "text-blue-500" : "text-muted-foreground"}`}
              onClick={() => setAutoScroll(!autoScroll)}
              aria-label={autoScroll ? "Disable auto-scroll" : "Enable auto-scroll"}
              title={autoScroll ? "Auto-scroll ON — click to disable" : "Auto-scroll OFF — click to enable"}
            >
              <ArrowDownToLine className="h-3 w-3" />
            </Button>
            {collapsible && (
              <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => setExpanded(!expanded)} aria-label={expanded ? "Collapse logs" : "Expand logs"} aria-expanded={expanded}>
                {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
              </Button>
            )}
            <Button variant="ghost" size="sm" className="h-6 px-1.5" onClick={handleCopy} aria-label="Copy logs to clipboard" title="Copy logs">
              {copied ? <Check className="h-3 w-3 text-green-500" /> : <ClipboardCopy className="h-3 w-3" />}
            </Button>
            <Button variant="ghost" size="sm" className="h-6 px-1.5" onClick={handleDownload} aria-label="Download logs" title="Download logs">
              <Download className="h-3 w-3" />
            </Button>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div
          ref={scrollRef}
          role="log"
          aria-live="polite"
          aria-label="Operation log output"
          className={`bg-[#0d1117] text-gray-300 p-3 rounded-lg font-mono text-xs overflow-y-auto ${heightClass}`}
          onWheel={() => {
            // User scrolled manually — disable auto-scroll
            if (autoScroll && scrollRef.current) {
              const { scrollTop, scrollHeight, clientHeight } = scrollRef.current;
              if (scrollHeight - scrollTop - clientHeight > 50) {
                setAutoScroll(false);
              }
            }
          }}
        >
          {filteredLogs.map((line, i) => (
            <div key={i} className={logColor(line)}>{line}</div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
