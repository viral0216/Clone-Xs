// @ts-nocheck
import { ReactNode } from "react";
import { HelpCircle } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cloneOptionHints } from "@/data/cloneOptionHints";

interface Props {
  /**
   * Config field key — used to look up the hint from cloneOptionHints.
   * When the key is missing from the hints map the info icon is hidden.
   */
  field?: string;
  /** Optional explicit hint that overrides the cloneOptionHints lookup. */
  hint?: string;
  /** Label visible to the user. Can be a string or a React node. */
  children: ReactNode;
  /** Pass-through className for the wrapping <label>. */
  className?: string;
}

const BASE = "text-sm font-medium inline-flex items-center gap-1.5";
const SMALL = "text-xs text-gray-500 inline-flex items-center gap-1.5";

export default function FieldLabel({ field, hint, children, className }: Props) {
  const resolved = hint ?? (field ? cloneOptionHints[field] : undefined);

  const content = (
    <span className={className ?? BASE}>
      {children}
      {resolved && (
        <Tooltip>
          {/* base-ui's Tooltip.Trigger always renders its own <button>
              and does NOT support radix-style asChild. Use it as the
              button directly — passing button props through. Wrapping
              another <button> inside (with asChild) produces nested
              buttons + an asChild attribute that leaks to the DOM. */}
          <TooltipTrigger
            type="button"
            tabIndex={-1}
            aria-label={`What is ${field || "this"}?`}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <HelpCircle className="h-3.5 w-3.5" />
          </TooltipTrigger>
          <TooltipContent side="top" className="max-w-xs whitespace-normal leading-snug">
            {resolved}
          </TooltipContent>
        </Tooltip>
      )}
    </span>
  );

  return content;
}

export function FieldLabelSmall(props: Props) {
  return <FieldLabel {...props} className={props.className ?? SMALL} />;
}

/** Standalone info icon + tooltip — for inline use inside existing labels (e.g. checkboxes). */
export function InfoDot({ field, hint }: { field?: string; hint?: string }) {
  const resolved = hint ?? (field ? cloneOptionHints[field] : undefined);
  if (!resolved) return null;
  return (
    <Tooltip>
      {/* See FieldLabel above — base-ui Trigger is the button itself. */}
      <TooltipTrigger
        type="button"
        tabIndex={-1}
        onClick={(e) => e.preventDefault()}
        aria-label={`What is ${field || "this"}?`}
        className="text-muted-foreground hover:text-foreground transition-colors"
      >
        <HelpCircle className="h-3 w-3" />
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs whitespace-normal leading-snug">
        {resolved}
      </TooltipContent>
    </Tooltip>
  );
}
