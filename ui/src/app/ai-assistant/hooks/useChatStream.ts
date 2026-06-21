import { useCallback, useRef, useState } from "react";

export interface ToolCallStep {
  call_id: string;
  tool: string;
  args: Record<string, unknown>;
  status: "running" | "done";
  result_preview?: string;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  streaming?: boolean;
  tool_steps?: ToolCallStep[];
  context_pruned?: boolean;
  total_tokens?: number;
  tool_count?: number;
}

function getAuthHeaders(): Record<string, string> {
  const host = localStorage.getItem("dbx_host") || "";
  const token = localStorage.getItem("dbx_token") || "";
  const sessionId = localStorage.getItem("clxs_session_id") || "";
  const aiModel = localStorage.getItem("dbx_model") || "";
  const warehouse = localStorage.getItem("dbx_warehouse_id") || "";
  return {
    ...(sessionId && { "X-Clone-Session": sessionId }),
    ...(host && { "X-Databricks-Host": host }),
    ...(token && { "X-Databricks-Token": token }),
    ...(aiModel && { "X-Databricks-Model": aiModel }),
    ...(warehouse && { "X-Databricks-Warehouse": warehouse }),
  };
}

export function useChatStream() {
  const [messages, setMessages]   = useState<ChatMessage[]>([]);
  const [streaming, setStreaming] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [error, setError]         = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const send = useCallback(
    async (
      text: string,
      mode: string,
      catalog?: string,
      schemaName?: string,
      regenerate = false,
    ) => {
      setError(null);
      setStreaming(true);
      setMessages((prev) => [
        ...prev,
        { role: "user",      content: text },
        { role: "assistant", content: "", streaming: true },
      ]);

      abortRef.current = new AbortController();

      try {
        const res = await fetch("/api/ai-assistant/stream", {
          method:  "POST",
          headers: { "Content-Type": "application/json", ...getAuthHeaders() },
          body:    JSON.stringify({
            message:    text,
            session_id: sessionId,
            mode,
            catalog:     catalog     || null,
            schema_name: schemaName  || null,
            regenerate,
          }),
          signal: abortRef.current.signal,
        });

        if (!res.ok) {
          const err = await res.json().catch(() => ({ detail: res.statusText }));
          throw new Error(err.detail || `HTTP ${res.status}`);
        }

        const reader  = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          const parts = buffer.split("\n\n");
          buffer = parts.pop() ?? "";

          for (const part of parts) {
            const line = part.trim();
            if (!line.startsWith("data: ")) continue;
            const raw = line.slice(6);
            let evt: Record<string, any>;
            try { evt = JSON.parse(raw); } catch { continue; }

            if (evt.type === "session_id") {
              setSessionId(evt.session_id);
            } else if (evt.type === "text") {
              setMessages((prev) => {
                const copy = [...prev];
                const last = { ...copy[copy.length - 1] };
                last.content += evt.delta;
                copy[copy.length - 1] = last;
                return copy;
              });
            } else if (evt.type === "tool_start") {
              setMessages((prev) => {
                const copy = [...prev];
                const last = { ...copy[copy.length - 1] };
                const step: ToolCallStep = {
                  call_id: evt.call_id,
                  tool: evt.tool,
                  args: evt.args ?? {},
                  status: "running",
                };
                last.tool_steps = [...(last.tool_steps ?? []), step];
                copy[copy.length - 1] = last;
                return copy;
              });
            } else if (evt.type === "tool_done") {
              setMessages((prev) => {
                const copy = [...prev];
                const last = { ...copy[copy.length - 1] };
                last.tool_steps = (last.tool_steps ?? []).map((s) =>
                  s.call_id === evt.call_id
                    ? { ...s, status: "done" as const, result_preview: evt.result_preview }
                    : s
                );
                copy[copy.length - 1] = last;
                return copy;
              });
            } else if (evt.type === "context_pruned") {
              setMessages((prev) => {
                const copy = [...prev];
                copy[copy.length - 1] = { ...copy[copy.length - 1], context_pruned: true };
                return copy;
              });
            } else if (evt.type === "usage") {
              setMessages((prev) => {
                const copy = [...prev];
                copy[copy.length - 1] = {
                  ...copy[copy.length - 1],
                  total_tokens: Number(evt.total_tokens) || 0,
                  tool_count: Number(evt.tool_count) || 0,
                };
                return copy;
              });
            } else if (evt.type === "done") {
              setMessages((prev) => {
                const copy = [...prev];
                copy[copy.length - 1] = { ...copy[copy.length - 1], streaming: false };
                return copy;
              });
              setStreaming(false);
            } else if (evt.type === "error") {
              setError(evt.message);
              setStreaming(false);
            }
          }
        }
      } catch (err: any) {
        if (err.name !== "AbortError") {
          setError(err.message || "Stream failed");
        }
        setMessages((prev) => {
          const copy = [...prev];
          if (copy.length && copy[copy.length - 1].streaming) {
            copy[copy.length - 1] = { ...copy[copy.length - 1], streaming: false };
          }
          return copy;
        });
        setStreaming(false);
      }
    },
    [sessionId],
  );

  const stop = useCallback(() => {
    abortRef.current?.abort();
    setStreaming(false);
    setMessages((prev) => {
      const copy = [...prev];
      if (copy.length && copy[copy.length - 1].streaming) {
        copy[copy.length - 1] = { ...copy[copy.length - 1], streaming: false };
      }
      return copy;
    });
  }, []);

  // Re-run the last user prompt, replacing the last assistant answer.
  const regenerate = useCallback(
    (mode: string, catalog?: string, schemaName?: string) => {
      if (streaming) return;
      let lastUser = "";
      for (let i = messages.length - 1; i >= 0; i--) {
        if (messages[i].role === "user") { lastUser = messages[i].content; break; }
      }
      if (!lastUser) return;
      // Drop the trailing assistant + user turns; send() re-adds the user turn.
      setMessages((prev) => {
        const copy = [...prev];
        if (copy.length && copy[copy.length - 1].role === "assistant") copy.pop();
        if (copy.length && copy[copy.length - 1].role === "user") copy.pop();
        return copy;
      });
      void send(lastUser, mode, catalog, schemaName, true);
    },
    [messages, streaming, send],
  );

  const reset = useCallback(() => {
    abortRef.current?.abort();
    setMessages([]);
    setSessionId(null);
    setError(null);
    setStreaming(false);
  }, []);

  const loadSession = useCallback((session: { id: string; messages: ChatMessage[] }) => {
    setSessionId(session.id);
    setMessages(session.messages);
    setError(null);
    setStreaming(false);
  }, []);

  return { messages, streaming, sessionId, error, send, stop, reset, loadSession, regenerate };
}
