"use client";

import { KeyboardEvent, useRef, useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import {
  ArrowUp,
  Square,
  Sparkles,
  BarChart2,
  Code2,
  FolderSearch,
  ShieldCheck,
  GitBranch,
  FlaskConical,
  Bot,
  Database,
  type LucideIcon,
} from "lucide-react";
import type { AgentMode } from "../hooks/useAgents";

export type { AgentMode };
export type AgentModeValue = string;

// Maps icon name strings (from .md frontmatter) → Lucide components.
// Add an entry here when you use a new icon name in an agent .md file.
export const ICON_MAP: Record<string, LucideIcon> = {
  Sparkles,
  BarChart2,
  Code2,
  FolderSearch,
  ShieldCheck,
  GitBranch,
  FlaskConical,
  Bot,
};

interface ChatInputProps {
  onSend: (text: string, mode: string) => void;
  onStop: () => void;
  streaming: boolean;
  disabled?: boolean;
  agents: AgentMode[];
  catalog?: string;
  onCatalogChange?: (c: string) => void;
  catalogs?: string[];
  schemaName?: string;
  onSchemaChange?: (s: string) => void;
  schemas?: string[];
  tables?: string[];
  onModeChange?: (mode: string) => void;
}

export function ChatInput({
  onSend, onStop, streaming, disabled,
  agents,
  catalog, onCatalogChange, catalogs = [],
  schemaName, onSchemaChange, schemas = [],
  tables = [],
  onModeChange,
}: ChatInputProps) {
  const [text, setText] = useState("");
  const [mode, setMode] = useState<string>("assistant");
  const [history, setHistory] = useState<string[]>([]);
  const historyIndexRef = useRef(-1);
  const savedInputRef = useRef("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // @-mention autocomplete state
  const [mention, setMention] = useState<{ query: string; start: number } | null>(null);
  const [mentionIdx, setMentionIdx] = useState(0);

  const mentionMatches = mention
    ? tables.filter((t) => t.toLowerCase().includes(mention.query.toLowerCase())).slice(0, 8)
    : [];

  const activeMode = agents.find((m) => m.value === mode) ?? agents[0];

  // Detect an @-token at the cursor and open the mention picker.
  const onTextChange = (value: string, cursor: number) => {
    setText(value);
    const upto = value.slice(0, cursor);
    const m = /(?:^|\s)@([\w.]*)$/.exec(upto);
    if (m && tables.length > 0) {
      setMention({ query: m[1], start: cursor - m[1].length - 1 });
      setMentionIdx(0);
    } else {
      setMention(null);
    }
  };

  const applyMention = (table: string) => {
    if (!mention) return;
    const el = textareaRef.current;
    const cursor = el?.selectionStart ?? text.length;
    const fq = [catalog, schemaName, table].filter(Boolean).join(".");
    const next = text.slice(0, mention.start) + fq + " " + text.slice(cursor);
    setText(next);
    setMention(null);
    requestAnimationFrame(() => {
      const pos = mention.start + fq.length + 1;
      el?.focus();
      el?.setSelectionRange(pos, pos);
    });
  };

  const changeMode = (v: string) => {
    setMode(v);
    onModeChange?.(v);
  };

  const submit = useCallback(() => {
    const trimmed = text.trim();
    if (!trimmed || streaming) return;
    setHistory(prev => [...prev, trimmed]);
    historyIndexRef.current = -1;
    savedInputRef.current = "";
    onSend(trimmed, mode);
    setText("");
    textareaRef.current?.focus();
  }, [text, streaming, mode, onSend]);

  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    // Mention picker navigation takes priority while open
    if (mention && mentionMatches.length > 0) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setMentionIdx((i) => (i + 1) % mentionMatches.length);
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setMentionIdx((i) => (i - 1 + mentionMatches.length) % mentionMatches.length);
        return;
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault();
        applyMention(mentionMatches[mentionIdx]);
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        setMention(null);
        return;
      }
    }

    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      submit();
      return;
    }

    const el = textareaRef.current;
    if (!el) return;

    if (e.key === "ArrowUp" && !e.shiftKey && el.selectionStart === 0 && el.selectionEnd === 0) {
      if (history.length === 0) return;
      e.preventDefault();
      if (historyIndexRef.current === -1) {
        savedInputRef.current = text;
        historyIndexRef.current = history.length - 1;
      } else if (historyIndexRef.current > 0) {
        historyIndexRef.current -= 1;
      }
      const next = history[historyIndexRef.current];
      setText(next);
      requestAnimationFrame(() => el.setSelectionRange(next.length, next.length));
      return;
    }

    if (e.key === "ArrowDown" && !e.shiftKey && historyIndexRef.current !== -1 &&
        el.selectionStart === el.value.length && el.selectionEnd === el.value.length) {
      e.preventDefault();
      if (historyIndexRef.current < history.length - 1) {
        historyIndexRef.current += 1;
        const next = history[historyIndexRef.current];
        setText(next);
        requestAnimationFrame(() => el.setSelectionRange(next.length, next.length));
      } else {
        historyIndexRef.current = -1;
        const restored = savedInputRef.current;
        setText(restored);
        requestAnimationFrame(() => el.setSelectionRange(restored.length, restored.length));
      }
    }
  };

  return (
    <div className="border-t border-border bg-background/95 backdrop-blur">
      {/* Mode pills — sourced dynamically from /api/ai-assistant/agents */}
      <div className="flex items-center gap-1 px-3 pt-2.5 pb-0 overflow-x-auto scrollbar-none">
        {agents.map((m) => {
          const Icon = ICON_MAP[m.icon] ?? Bot;
          const active = mode === m.value;
          return (
            <button
              key={m.value}
              onClick={() => changeMode(m.value)}
              className={cn(
                "flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-medium whitespace-nowrap transition-colors shrink-0",
                active
                  ? "bg-primary text-primary-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted",
              )}
            >
              <Icon className={cn("h-3 w-3", active ? "text-primary-foreground" : m.color)} />
              {m.label}
            </button>
          );
        })}
      </div>

      {/* Input card */}
      <div className="px-3 py-2 relative">
        {/* @-mention table picker */}
        {mention && mentionMatches.length > 0 && (
          <div className="absolute bottom-full left-3 mb-1 w-72 max-h-56 overflow-y-auto rounded-lg border border-border bg-popover shadow-lg z-50 py-1">
            <p className="px-2.5 py-1 text-[10px] text-muted-foreground border-b border-border/50">
              Tables in {catalog}.{schemaName}
            </p>
            {mentionMatches.map((t, i) => (
              <button
                key={t}
                onMouseDown={(e) => { e.preventDefault(); applyMention(t); }}
                onMouseEnter={() => setMentionIdx(i)}
                className={cn(
                  "flex items-center gap-2 w-full text-left px-2.5 py-1.5 text-xs",
                  i === mentionIdx ? "bg-primary/10 text-primary" : "hover:bg-muted",
                )}
              >
                <Database className="h-3 w-3 shrink-0 opacity-60" />
                <span className="font-mono truncate">{t}</span>
              </button>
            ))}
          </div>
        )}
        <div className={cn(
          "rounded-xl border transition-colors",
          streaming ? "border-primary/40 bg-primary/5" : "border-border bg-muted/30 focus-within:border-primary/50 focus-within:bg-background",
        )}>
          <Textarea
            ref={textareaRef}
            value={text}
            onChange={(e) => onTextChange(e.target.value, e.target.selectionStart ?? e.target.value.length)}
            onKeyDown={onKey}
            placeholder={streaming ? `${activeMode?.label ?? "Assistant"} is responding…` : `Ask ${activeMode?.label.toLowerCase() ?? "anything"}…`}
            rows={1}
            disabled={disabled || streaming}
            className="resize-none min-h-[44px] max-h-36 text-sm border-0 bg-transparent shadow-none focus-visible:ring-0 px-3 pt-3 pb-1 leading-relaxed"
          />

          {/* Footer row inside the card */}
          <div className="flex items-center justify-between px-2 pb-2 gap-2">
            <div className="flex items-center gap-1.5 min-w-0">
              {/* Catalog / schema pickers — use portal-based Select to avoid overflow-hidden clipping */}
              {catalogs.length > 0 && (
                <Select
                  value={catalog || ""}
                  onValueChange={(v) => { onCatalogChange?.(v ?? ""); onSchemaChange?.(""); }}
                >
                  <SelectTrigger size="sm" className="h-6 text-[11px] min-w-[90px] max-w-[140px] border-border/60 bg-muted/40">
                    <SelectValue placeholder="All catalogs" />
                  </SelectTrigger>
                  <SelectContent align="start" side="top">
                    <SelectItem value="">All catalogs</SelectItem>
                    {catalogs.map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
                  </SelectContent>
                </Select>
              )}
              {catalog && schemas.length > 0 && (
                <Select
                  value={schemaName || ""}
                  onValueChange={(v) => onSchemaChange?.(v ?? "")}
                >
                  <SelectTrigger size="sm" className="h-6 text-[11px] min-w-[80px] max-w-[130px] border-border/60 bg-muted/40">
                    <SelectValue placeholder="All schemas" />
                  </SelectTrigger>
                  <SelectContent align="start" side="top">
                    <SelectItem value="">All schemas</SelectItem>
                    {schemas.map((s) => <SelectItem key={s} value={s}>{s}</SelectItem>)}
                  </SelectContent>
                </Select>
              )}
            </div>

            <div className="flex items-center gap-1.5 shrink-0">
              <span className="text-[10px] text-muted-foreground hidden sm:inline">⌤ send</span>
              {streaming ? (
                <Button size="icon" variant="ghost" className="h-7 w-7 rounded-lg hover:bg-destructive/10 hover:text-destructive" onClick={onStop}>
                  <Square className="h-3.5 w-3.5 fill-current" />
                </Button>
              ) : (
                <Button
                  size="icon"
                  className="h-7 w-7 rounded-lg"
                  disabled={!text.trim() || disabled}
                  onClick={submit}
                >
                  <ArrowUp className="h-3.5 w-3.5" />
                </Button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
