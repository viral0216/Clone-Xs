// @ts-nocheck
import { Component, ReactNode } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AlertTriangle } from "lucide-react";

interface Props {
  children: ReactNode;
  fallbackTitle?: string;
}

interface State {
  error: Error | null;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  handleReset = () => {
    this.setState({ error: null });
  };

  render() {
    if (this.state.error) {
      return (
        <Card className="border-red-200 dark:border-red-900/50">
          <CardHeader className="flex flex-row items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
            <CardTitle>{this.props.fallbackTitle || "Something went wrong"}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <p className="text-sm text-muted-foreground">
              {this.state.error.message || "An unexpected error occurred while rendering this page."}
            </p>
            <div className="flex gap-2">
              <Button variant="outline" onClick={this.handleReset}>Try again</Button>
              <Button variant="outline" onClick={() => window.location.reload()}>Reload page</Button>
            </div>
          </CardContent>
        </Card>
      );
    }
    return this.props.children;
  }
}
