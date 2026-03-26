/**
 * CloneBuilder — Natural language clone configuration builder.
 *
 * Allows users to describe a clone operation in plain English,
 * then parses it into a structured config using AI.
 */

import { useState } from "react";
import { Sparkles, ArrowRight, Loader2, X, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useCloneBuilder, useAiStatus } from "@/hooks/useAi";
import { useNavigate } from "react-router-dom";

interface CloneBuilderProps {
  onClose: () => void;
}

export default function CloneBuilder({ onClose }: CloneBuilderProps) {
  const navigate = useNavigate();
  const aiStatus = useAiStatus();
  const cloneBuilder = useCloneBuilder();
  const [query, setQuery] = useState("");
  const [copied, setCopied] = useState(false);

  if (!aiStatus.data?.available) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    cloneBuilder.mutate({ query: query.trim() });
  };

  const handleApply = () => {
    // Navigate to clone page with config in state
    if (cloneBuilder.data?.config) {
      navigate("/clone", { state: { aiConfig: cloneBuilder.data.config } });
      onClose();
    }
  };

  const handleCopyConfig = () => {
    if (cloneBuilder.data?.config) {
      navigator.clipboard.writeText(JSON.stringify(cloneBuilder.data.config, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-[15vh] bg-black/50 backdrop-blur-sm" onClick={onClose}>
      <div
        className="w-full max-w-lg bg-background border border-border rounded-xl shadow-2xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-muted/30">
          <div className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-primary" />
            <span className="text-sm font-medium">AI Clone Builder</span>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Input */}
        <form onSubmit={handleSubmit} className="p-4">
          <div className="relative">
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Describe your clone operation in plain English...&#10;&#10;Examples:&#10;• Clone production catalog to dev, skip the raw schema&#10;• Create a shallow copy of analytics for testing&#10;• Mirror prod to dr-backup with all permissions"
              className="w-full h-28 px-3 py-2.5 text-sm bg-muted/30 border border-border rounded-lg resize-none focus:outline-none focus:ring-1 focus:ring-primary placeholder:text-muted-foreground/50"
              autoFocus
            />
          </div>
          <Button
            type="submit"
            size="sm"
            className="mt-2 w-full"
            disabled={!query.trim() || cloneBuilder.isPending}
          >
            {cloneBuilder.isPending ? (
              <>
                <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                Parsing...
              </>
            ) : (
              <>
                <Sparkles className="h-3.5 w-3.5 mr-1.5" />
                Build Clone Config
              </>
            )}
          </Button>
        </form>

        {/* Results */}
        {cloneBuilder.data && (
          <div className="px-4 pb-4 space-y-3">
            {cloneBuilder.data.explanation && (
              <p className="text-xs text-muted-foreground">
                {cloneBuilder.data.explanation}
              </p>
            )}

            {cloneBuilder.data.config && Object.keys(cloneBuilder.data.config).length > 0 && (
              <>
                <pre className="text-xs bg-muted/40 rounded-lg p-3 overflow-x-auto max-h-48 font-mono">
                  {JSON.stringify(cloneBuilder.data.config, null, 2)}
                </pre>

                <div className="flex gap-2">
                  <Button size="sm" onClick={handleApply} className="flex-1">
                    <ArrowRight className="h-3.5 w-3.5 mr-1.5" />
                    Apply to Clone Page
                  </Button>
                  <Button size="sm" variant="outline" onClick={handleCopyConfig}>
                    {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
                  </Button>
                </div>
              </>
            )}

            {cloneBuilder.data.reason && (
              <p className="text-xs text-destructive">{cloneBuilder.data.reason}</p>
            )}
          </div>
        )}

        {cloneBuilder.isError && (
          <div className="px-4 pb-4">
            <p className="text-xs text-destructive">
              Failed to parse: {cloneBuilder.error?.message}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
