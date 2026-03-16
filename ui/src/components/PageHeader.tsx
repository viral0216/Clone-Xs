// @ts-nocheck
import { ExternalLink } from "lucide-react";

interface PageHeaderProps {
  title: string;
  description?: string;
  icon?: React.ComponentType<{ className?: string }>;
  docsUrl?: string;
  docsLabel?: string;
  breadcrumbs?: string[];
  actions?: React.ReactNode;
}

export default function PageHeader({
  title,
  description,
  icon: Icon,
  docsUrl,
  docsLabel,
  breadcrumbs,
  actions,
}: PageHeaderProps) {
  return (
    <div className="mb-6">
      {/* Breadcrumbs — 12px, muted */}
      {breadcrumbs && breadcrumbs.length > 0 && (
        <div className="flex items-center gap-1.5 text-[12px] text-muted-foreground mb-2">
          <span className="font-bold">Clone<span className="text-[#E8453C] mx-0.5">→</span>Xs</span>
          {breadcrumbs.map((crumb, i) => (
            <span key={i} className="flex items-center gap-1.5">
              <span className="text-muted-foreground/40">&rsaquo;</span>
              <span className={i === breadcrumbs.length - 1 ? "text-foreground font-medium" : ""}>{crumb}</span>
            </span>
          ))}
        </div>
      )}

      {/* Title row */}
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            {Icon && <Icon className="h-5 w-5 text-gray-500 dark:text-gray-400 shrink-0" />}
            <h1 className="text-foreground">{title}</h1>
          </div>

          {description && (
            <p className="text-[13px] text-muted-foreground mt-1 leading-relaxed max-w-3xl">
              {description}
            </p>
          )}

          {docsUrl && (
            <a
              href={docsUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-[12px] text-blue-600 hover:text-blue-700 hover:underline mt-1 transition-colors"
            >
              <ExternalLink className="h-3 w-3" />
              {docsLabel || "Documentation"}
            </a>
          )}
        </div>

        {actions && (
          <div className="flex items-center gap-2 shrink-0">
            {actions}
          </div>
        )}
      </div>
    </div>
  );
}
