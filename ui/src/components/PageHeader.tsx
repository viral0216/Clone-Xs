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
        <nav aria-label="Breadcrumb" className="mb-2">
          <ol className="flex items-center gap-1.5 text-[12px] text-muted-foreground list-none p-0 m-0">
            <li className="inline-flex items-center gap-1">
              <svg className="h-3.5 w-3.5 shrink-0" viewBox="0 0 36 36" fill="none" aria-hidden="true">
                <rect x="6" y="6" width="22" height="24" rx="3" className="fill-[#E8453C]/10 stroke-[#E8453C]/30" strokeWidth="1.2"/>
                <rect x="0" y="0" width="22" height="24" rx="3" className="fill-white dark:fill-[#1a1a1a] stroke-gray-400 dark:stroke-gray-500" strokeWidth="1.2"/>
                <line x1="5" y1="6" x2="17" y2="6" className="stroke-gray-800 dark:stroke-gray-200" strokeWidth="1.2" strokeLinecap="round"/>
                <line x1="5" y1="10" x2="14" y2="10" className="stroke-gray-400 dark:stroke-gray-500" strokeWidth="1.2" strokeLinecap="round" opacity="0.5"/>
                <line x1="5" y1="14" x2="15" y2="14" className="stroke-gray-400 dark:stroke-gray-500" strokeWidth="1.2" strokeLinecap="round" opacity="0.5"/>
                <path d="M14 12L24 12" stroke="#E8453C" strokeWidth="1.5" strokeLinecap="round"/>
                <path d="M21 9L25 12L21 15" stroke="#E8453C" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              <span className="font-bold">Clone<span className="text-[#E8453C] mx-0.5">→</span>Xs</span>
            </li>
            {breadcrumbs.map((crumb, i) => (
              <li key={i} className="flex items-center gap-1.5">
                <span className="text-muted-foreground/40" aria-hidden="true">&rsaquo;</span>
                <span className={i === breadcrumbs.length - 1 ? "text-foreground font-medium" : ""} aria-current={i === breadcrumbs.length - 1 ? "page" : undefined}>{crumb}</span>
              </li>
            ))}
          </ol>
        </nav>
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
