// @ts-nocheck
"use client";

import { useCallback, useEffect, useState } from "react";
import { Sparkles, Cpu } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ChatThread } from "./components/ChatThread";
import { ChatInput } from "./components/ChatInput";
import { SessionSidebar } from "./components/SessionSidebar";
import { useChatStream } from "./hooks/useChatStream";
import { useAgents } from "./hooks/useAgents";
import { api } from "@/lib/api-client";

interface CatalogContext {
  catalogs: string[];
  schemas: string[];
  tables?: string[];
}

interface ModelEndpoint {
  name: string;
  state: string;
  provider: string;
}

export default function AiAssistantPage() {
  const { messages, streaming, sessionId, error, send, stop, reset, loadSession, regenerate } = useChatStream();
  const agents = useAgents();

  const [catalog, setCatalog]       = useState<string>(() => localStorage.getItem("dbx_catalog_filter") || "");
  const [schemaName, setSchemaName] = useState<string>("");
  const [context, setContext]       = useState<CatalogContext>({ catalogs: [], schemas: [] });
  const [modelName, setModelName]   = useState<string>("");
  const [models, setModels]         = useState<ModelEndpoint[]>([]);
  const [activeMode, setActiveMode] = useState("assistant");
  const [refreshSidebar, setRefreshSidebar] = useState(0);
  const [lastSentMessage, setLastSentMessage] = useState<string>("");

  const noModel = !modelName;

  useEffect(() => {
    setModelName(localStorage.getItem("dbx_model") || "");

    const host = localStorage.getItem("dbx_host");
    if (!host) return;
    api
      .get<{ success: boolean; endpoints: ModelEndpoint[] }>("/auth/serving-endpoints")
      .then((data) => { if (data.success) setModels(data.endpoints); })
      .catch(() => {});
  }, []);

  useEffect(() => {
    const host = localStorage.getItem("dbx_host");
    if (!host) return;
    const params: Record<string, string> = {};
    if (catalog) params.catalog = catalog;
    if (catalog && schemaName) params.schema_name = schemaName;
    api
      .get<CatalogContext>("/ai-assistant/context/databricks", { params })
      .then(setContext)
      .catch(() => {});
  }, [catalog, schemaName]);

  const handleModelChange = useCallback((name: string) => {
    localStorage.setItem("dbx_model", name);
    setModelName(name);
  }, []);

  const handleSend = useCallback(
    (text: string, mode: string) => {
      setLastSentMessage(text);
      send(text, mode, catalog || undefined, schemaName || undefined);
    },
    [send, catalog, schemaName],
  );

  const handleRunSavedPrompt = useCallback(
    (text: string) => {
      setLastSentMessage(text);
      send(text, activeMode, catalog || undefined, schemaName || undefined);
    },
    [send, activeMode, catalog, schemaName],
  );

  const handleNew = useCallback(() => {
    reset();
    setRefreshSidebar((n) => n + 1);
  }, [reset]);

  const handleSuggestedPrompt = useCallback(
    (text: string) => {
      send(text, activeMode, catalog || undefined, schemaName || undefined);
    },
    [send, activeMode, catalog, schemaName],
  );

  const handleRegenerate = useCallback(() => {
    regenerate(activeMode, catalog || undefined, schemaName || undefined);
  }, [regenerate, activeMode, catalog, schemaName]);

  useEffect(() => {
    if (!streaming && sessionId && messages.length > 0) {
      setRefreshSidebar((n) => n + 1);
    }
  }, [streaming, sessionId, messages.length]);

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <PageHeader
        title="AI Assistant"
        icon={Sparkles}
        breadcrumbs={["Discovery", "AI Assistant"]}
        description="Chat with your Databricks workspace — ask questions, write SQL, explore Unity Catalog."
      />

      <div className="flex flex-1 overflow-hidden border-t border-border">
        {/* Session sidebar */}
        <div className="w-[200px] shrink-0 border-r border-border overflow-hidden flex flex-col">
          <SessionSidebar
            activeSessionId={sessionId}
            onSelect={loadSession}
            onNew={handleNew}
            refreshTrigger={refreshSidebar}
            onRunPrompt={handleRunSavedPrompt}
            lastSentMessage={lastSentMessage}
            lastSentMode={activeMode}
          />
        </div>

        {/* Chat area */}
        <div className="flex-1 flex flex-col overflow-hidden min-w-0">
          {/* Model selector bar — always visible */}
          <div className="flex items-center justify-between px-4 py-1.5 border-b border-border bg-muted/10 shrink-0 gap-4">
            <div className="flex items-center gap-2 min-w-0">
              <Cpu className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
              {models.length > 0 ? (
                <Select value={modelName || ""} onValueChange={(v) => v && handleModelChange(v)}>
                  <SelectTrigger size="sm" className="h-6 text-[11px] min-w-[160px] max-w-[320px] border-border/60 bg-muted/40">
                    <SelectValue placeholder="Select a model…" />
                  </SelectTrigger>
                  <SelectContent align="start">
                    {models.map((m) => (
                      <SelectItem key={m.name} value={m.name}>{m.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <span className="text-[11px] text-muted-foreground font-medium truncate">
                  {modelName || "No model — connect to Databricks first"}
                </span>
              )}
            </div>
            {error && (
              <p className="text-[11px] text-destructive truncate shrink min-w-0">{error}</p>
            )}
          </div>

          <ChatThread
            messages={messages}
            agents={agents}
            catalog={catalog}
            schemaName={schemaName}
            activeMode={activeMode}
            onSuggestedPrompt={handleSuggestedPrompt}
            onRegenerate={handleRegenerate}
            streaming={streaming}
          />

          <ChatInput
            onSend={handleSend}
            onStop={stop}
            streaming={streaming}
            disabled={noModel}
            agents={agents}
            catalog={catalog}
            onCatalogChange={setCatalog}
            catalogs={context.catalogs}
            schemaName={schemaName}
            onSchemaChange={setSchemaName}
            schemas={context.schemas}
            tables={context.tables ?? []}
            onModeChange={setActiveMode}
          />
        </div>
      </div>
    </div>
  );
}
