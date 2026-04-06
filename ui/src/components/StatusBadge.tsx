import { Badge } from "@/components/ui/badge";
import { CheckCircle, XCircle, AlertTriangle, Loader2, HelpCircle } from "lucide-react";

const STATUS_CONFIG: Record<string, { icon: any; className: string; label?: string }> = {
  success: { icon: CheckCircle, className: "text-green-500 border-green-500/30" },
  completed: { icon: CheckCircle, className: "text-green-500 border-green-500/30" },
  fresh: { icon: CheckCircle, className: "text-green-500 border-green-500/30" },
  passed: { icon: CheckCircle, className: "text-green-500 border-green-500/30" },
  active: { icon: CheckCircle, className: "text-green-500 border-green-500/30" },
  failed: { icon: XCircle, className: "text-red-500 border-red-500/30" },
  error: { icon: XCircle, className: "text-red-500 border-red-500/30" },
  stale: { icon: XCircle, className: "text-red-500 border-red-500/30" },
  warning: { icon: AlertTriangle, className: "text-amber-500 border-amber-500/30" },
  running: { icon: Loader2, className: "text-blue-500 border-blue-500/30" },
  pending: { icon: Loader2, className: "text-blue-500 border-blue-500/30" },
  unknown: { icon: HelpCircle, className: "text-muted-foreground border-border" },
};

interface StatusBadgeProps {
  status: string;
  label?: string;
  className?: string;
}

export default function StatusBadge({ status, label, className = "" }: StatusBadgeProps) {
  const config = STATUS_CONFIG[status.toLowerCase()] || STATUS_CONFIG.unknown;
  const Icon = config.icon;
  const isSpinner = Icon === Loader2;
  return (
    <Badge variant="outline" className={`text-[10px] gap-1 ${config.className} ${className}`}>
      <Icon className={`h-3 w-3 ${isSpinner ? "animate-spin" : ""}`} />
      {label || status}
    </Badge>
  );
}
