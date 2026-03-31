import { AlertTriangle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorStateProps {
  title?: string;
  message?: string;
  onRetry?: () => void;
  retrying?: boolean;
}

export default function ErrorState({
  title = "Something went wrong",
  message = "Failed to load data. Please try again.",
  onRetry,
  retrying = false,
}: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12 space-y-3">
      <AlertTriangle className="h-10 w-10 text-muted-foreground/40" />
      <p className="text-sm font-medium text-foreground">{title}</p>
      <p className="text-xs text-muted-foreground max-w-md text-center">{message}</p>
      {onRetry && (
        <Button variant="outline" size="sm" onClick={onRetry} disabled={retrying}>
          <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${retrying ? "animate-spin" : ""}`} />
          {retrying ? "Retrying..." : "Try Again"}
        </Button>
      )}
    </div>
  );
}
