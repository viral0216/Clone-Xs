import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { XCircle, RefreshCw } from "lucide-react";

interface ErrorCardProps {
  error: string;
  onRetry?: () => void;
  className?: string;
}

export default function ErrorCard({ error, onRetry, className = "" }: ErrorCardProps) {
  return (
    <Card className={`border-red-500/30 bg-red-500/5 ${className}`}>
      <CardContent className="flex items-start gap-3 pt-4">
        <XCircle className="h-5 w-5 text-red-500 shrink-0 mt-0.5" />
        <div className="flex-1 min-w-0">
          <p className="text-sm text-red-500 font-medium">Error</p>
          <p className="text-xs text-red-400 mt-0.5 break-words">{error}</p>
        </div>
        {onRetry && (
          <Button variant="outline" size="sm" onClick={onRetry} className="shrink-0">
            <RefreshCw className="h-3 w-3 mr-1" /> Retry
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
