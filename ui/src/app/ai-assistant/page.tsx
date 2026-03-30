// @ts-nocheck
import { useState, useRef, useEffect } from "react";
import { useMutation } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { api } from "@/lib/api-client";
import {
  Sparkles, Send, Database, Code, Copy,
  Loader2, Table2, ChevronDown, Trash2, Wand2,
} from "lucide-react";

type Message = { role: "user" | "assistant" | "system"; content: string; sql?: string; results?: any[]; rowCount?: number };

export default function AiAssistantPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [mode, setMode] = useState<"ai" | "genie">("ai");
  const [genieSpaceId, setGenieSpaceId] = useState(() => localStorage.getItem("dbx_genie_space_id") || "");
  const [endpoints, setEndpoints] = useState<any[]>([]);
  const [selectedModel, setSelectedModel] = useState(() => localStorage.getItem("dbx_model") || "databricks-claude-opus-4-6");
  const [showResults, setShowResults] = useState<number | null>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  useEffect(() => {
    api.get("/auth/serving-endpoints").then((data: any) => {
      const eps = data.endpoints || [];
      setEndpoints(eps);
      // Auto-select first Claude endpoint if nothing saved
      if (!localStorage.getItem("dbx_model") && eps.length > 0) {
        const claude = eps.find((e: any) => e.is_claude && e.state?.includes("READY"));
        const first = claude || eps.find((e: any) => e.state?.includes("READY"));
        if (first) {
          setSelectedModel(first.name);
          localStorage.setItem("dbx_model", first.name);
        }
      }
    }).catch(() => {});
  }, []);

  useEffect(() => {
    api.get("/catalogs").then((data: any) => {
      setCatalogs(Array.isArray(data) ? data.map((c: any) => c.name || c) : []);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (!catalog) { setSchemas([]); return; }
    api.get(`/catalogs/${catalog}/schemas`).then((data: any) => {
      setSchemas(Array.isArray(data) ? data.map((s: any) => s.name || s) : []);
    }).catch(() => setSchemas([]));
  }, [catalog]);

  const executeNL = useMutation({
    mutationFn: (question: string) => api.post("/ai-assistant/execute-nl", { question, catalog, schema_name: schema }),
    onSuccess: (data: any) => {
      setMessages(prev => [...prev, {
        role: "assistant",
        content: data.error ? `Error: ${data.error}` : (data.explanation || "Query executed."),
        sql: data.sql, results: data.results, rowCount: data.row_count,
      }]);
    },
    onError: (err: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err.message}` }]);
    },
  });

  const generateSQL = useMutation({
    mutationFn: (question: string) => api.post("/ai-assistant/nl-to-sql", { question, catalog, schema_name: schema }),
    onSuccess: (data: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: data.error ? `Error: ${data.error}` : "Generated SQL:", sql: data.sql }]);
    },
    onError: (err: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err?.message || String(err)}` }]);
    },
  });

  const genieQuery = useMutation({
    mutationFn: (question: string) => api.post("/ai-assistant/genie-query", { question, space_id: genieSpaceId }),
    onSuccess: (data: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: data.error ? `Genie: ${data.error}` : (data.explanation || data.description || "Done."), sql: data.sql }]);
    },
    onError: (err: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: `Genie error: ${err?.message || String(err)}` }]);
    },
  });

  const chat = useMutation({
    mutationFn: (msgs: Message[]) => api.post("/ai-assistant/chat", { messages: msgs.map(m => ({ role: m.role, content: m.content })), catalog, schema_name: schema }),
    onSuccess: (data: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: data.error ? `Error: ${data.error}` : (data.response || "No response") }]);
    },
    onError: (err: any) => {
      setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err?.message || String(err)}` }]);
    },
  });

  const isLoading = executeNL.isPending || generateSQL.isPending || genieQuery.isPending || chat.isPending;

  const handleSend = () => {
    if (!input.trim() || isLoading) return;
    const question = input.trim();
    setInput("");
    const userMsg: Message = { role: "user", content: question };
    setMessages(prev => [...prev, userMsg]);

    if (mode === "genie") {
      genieQuery.mutate(question);
    } else {
      const isSqlRequest = /select|show|describe|list|count|find|get|what|how many|average|sum|top|table|column|schema/i.test(question);
      if (isSqlRequest) {
        executeNL.mutate(question);
      } else {
        chat.mutate([...messages, userMsg]);
      }
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
  };

  return (
    <div className="space-y-4">
      <PageHeader title="AI Assistant" icon={Sparkles} breadcrumbs={["Discovery", "AI Assistant"]}
        description="Ask questions about your data in natural language — AI generates SQL, executes it, and explains results." />

      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center bg-muted rounded-lg p-0.5 border border-border">
          <button onClick={() => setMode("ai")}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${mode === "ai" ? "bg-[#E8453C] text-white shadow-sm" : "text-muted-foreground hover:text-foreground"}`}>
            <Wand2 className="h-3 w-3" /> AI Model
          </button>
          <button onClick={() => setMode("genie")}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${mode === "genie" ? "bg-[#E8453C] text-white shadow-sm" : "text-muted-foreground hover:text-foreground"}`}>
            <Sparkles className="h-3 w-3" /> Genie
          </button>
        </div>

        <Database className="h-3.5 w-3.5 text-muted-foreground" />
        <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md min-w-[120px]" value={catalog} onChange={e => { setCatalog(e.target.value); setSchema(""); }}>
          <option value="">All Catalogs</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md min-w-[120px]" value={schema} onChange={e => setSchema(e.target.value)} disabled={!catalog}>
          <option value="">All Schemas</option>
          {schemas.map(s => <option key={s} value={s}>{s}</option>)}
        </select>

        {mode === "genie" && (
          <input className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md w-48" placeholder="Genie Space ID"
            value={genieSpaceId} onChange={e => { setGenieSpaceId(e.target.value); localStorage.setItem("dbx_genie_space_id", e.target.value); }} />
        )}

        {mode === "ai" && (
          <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md min-w-[200px]"
            value={selectedModel}
            onChange={e => {
              const val = e.target.value;
              setSelectedModel(val);
              if (val) {
                localStorage.setItem("dbx_model", val);
              } else {
                localStorage.removeItem("dbx_model");
              }
            }}>
            <option value="">Anthropic API (Direct)</option>
            {endpoints.map(ep => (
              <option key={ep.name} value={ep.name}>
                {ep.name} {ep.is_claude ? "[CLAUDE]" : ""} - {String(ep.state).replace("EndpointStateReady.", "")}
              </option>
            ))}
          </select>
        )}

        {messages.length > 0 && (
          <Button size="sm" variant="ghost" className="ml-auto h-7 text-xs" onClick={() => setMessages([])}><Trash2 className="h-3 w-3 mr-1" /> Clear</Button>
        )}
      </div>

      {/* Input area — at the top for visibility */}
      <div className="border border-border rounded-lg bg-muted/20 p-3">
        <div className="flex items-end gap-2">
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={mode === "genie" ? "Ask Genie about your data..." : "Ask about your data — e.g. 'Show me the top 10 largest tables in edp_dev'"}
            rows={2}
            className="flex-1 px-3 py-2 text-sm bg-background border border-border rounded-md resize-none focus:outline-none focus:ring-2 focus:ring-[#E8453C]/20 focus:border-[#E8453C]"
          />
          <div className="flex flex-col gap-1">
            {mode === "ai" && (
              <Button size="sm" variant="outline" className="h-8 text-xs" disabled={!input.trim() || isLoading}
                onClick={() => { const q = input.trim(); setInput(""); setMessages(prev => [...prev, { role: "user", content: q }]); generateSQL.mutate(q); }}>
                <Code className="h-3 w-3 mr-1" /> SQL Only
              </Button>
            )}
            <Button size="sm" className="h-8 bg-[#E8453C] hover:bg-[#E8453C]/90" disabled={!input.trim() || isLoading} onClick={handleSend}>
              {isLoading ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Send className="h-3 w-3 mr-1" />}
              {mode === "genie" ? "Ask Genie" : "Ask AI"}
            </Button>
          </div>
        </div>
        <p className="text-[10px] text-muted-foreground mt-1.5">Enter to send, Shift+Enter for new line</p>
      </div>

      {/* Suggested questions */}
      {messages.length === 0 && (
        <div className="flex flex-wrap gap-2">
          {[
            "Show me the top 10 largest tables",
            "How many rows are in each schema?",
            "What tables have PII columns?",
            "List all catalogs and their sizes",
            "Show recent clone operations",
            "Describe the columns in the customers table",
          ].map(q => (
            <button key={q} onClick={() => { setInput(q); inputRef.current?.focus(); }}
              className="px-3 py-1.5 text-xs bg-muted/50 border border-border rounded-lg hover:bg-muted hover:border-[#E8453C]/30 transition-colors">
              {q}
            </button>
          ))}
        </div>
      )}

      {/* Chat messages */}
      {messages.map((msg, i) => (
        <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
          <div className={`max-w-[85%] rounded-lg px-4 py-3 ${
            msg.role === "user" ? "bg-[#E8453C] text-white" : "bg-card border border-border"
          }`}>
            <p className="text-sm whitespace-pre-wrap">{msg.content}</p>

            {msg.sql && (
              <div className="mt-2 rounded-md bg-background border border-border overflow-hidden">
                <div className="flex items-center justify-between px-3 py-1.5 bg-muted/50 border-b border-border">
                  <span className="text-[10px] font-medium text-muted-foreground flex items-center gap-1"><Code className="h-3 w-3" /> SQL</span>
                  <button onClick={() => navigator.clipboard.writeText(msg.sql!)} className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
                    <Copy className="h-2.5 w-2.5" /> Copy
                  </button>
                </div>
                <pre className="px-3 py-2 text-xs font-mono overflow-x-auto text-foreground">{msg.sql}</pre>
              </div>
            )}

            {msg.results && msg.results.length > 0 && (
              <div className="mt-2">
                <button onClick={() => setShowResults(showResults === i ? null : i)}
                  className="flex items-center gap-1.5 text-[10px] text-muted-foreground hover:text-foreground">
                  <Table2 className="h-3 w-3" /> {msg.rowCount} rows
                  <ChevronDown className={`h-3 w-3 transition-transform ${showResults === i ? "rotate-180" : ""}`} />
                </button>
                {showResults === i && (
                  <div className="mt-1.5 rounded-md border border-border overflow-x-auto max-h-60 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="bg-muted/50">
                          {Object.keys(msg.results[0]).map(col => (
                            <th key={col} className="px-2 py-1.5 text-left font-medium text-muted-foreground whitespace-nowrap">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {msg.results.slice(0, 20).map((row, ri) => (
                          <tr key={ri} className="border-t border-border">
                            {Object.values(row).map((val: any, ci) => (
                              <td key={ci} className="px-2 py-1.5 whitespace-nowrap">{val == null ? <span className="text-muted-foreground/40">NULL</span> : String(val)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      ))}

      {isLoading && (
        <div className="flex justify-start">
          <div className="bg-card border border-border rounded-lg px-4 py-3 flex items-center gap-2">
            <Loader2 className="h-3.5 w-3.5 animate-spin text-[#E8453C]" />
            <div>
              <span className="text-xs text-muted-foreground">{mode === "genie" ? "Asking Genie..." : "Generating SQL, executing, and explaining results..."}</span>
              <p className="text-[10px] text-muted-foreground mt-0.5">This may take 15-30 seconds via Databricks Model Serving</p>
            </div>
          </div>
        </div>
      )}

      <div ref={chatEndRef} />
    </div>
  );
}
