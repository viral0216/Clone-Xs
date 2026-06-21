"use client";

import { memo } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { SqlResultTable } from "./SqlResultTable";
import { ToolCallStepView } from "./ToolCallStep";
import { CopyButton } from "./CopyButton";
import { cn } from "@/lib/utils";
import { RefreshCw, Sparkles, User } from "lucide-react";
import type { ToolCallStep } from "../hooks/useChatStream";

interface MessageBubbleProps {
  role: "user" | "assistant";
  content: string;
  streaming?: boolean;
  catalog?: string;
  schemaName?: string;
  tool_steps?: ToolCallStep[];
  context_pruned?: boolean;
  total_tokens?: number;
  tool_count?: number;
  onSuggestionClick?: (text: string) => void;
  onRegenerate?: () => void;
  isLast?: boolean;
}

// Split content on fenced code blocks so SQL blocks get the Run Query button
// while everything else goes through react-markdown.
function splitOnCodeBlocks(content: string) {
  const parts: Array<{ type: "text" | "code"; lang: string; body: string }> = [];
  const RE = /```(\w*)\n([\s\S]*?)```/g;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = RE.exec(content)) !== null) {
    if (m.index > last) parts.push({ type: "text", lang: "", body: content.slice(last, m.index) });
    parts.push({ type: "code", lang: (m[1] || "").toLowerCase(), body: m[2].trim() });
    last = m.index + m[0].length;
  }
  if (last < content.length) parts.push({ type: "text", lang: "", body: content.slice(last) });
  return parts;
}

// Strip the trailing ```next-steps block so it isn't included when copying.
function stripBlocks(content: string): string {
  return content.replace(/```next-steps\n[\s\S]*?```/g, "").trim();
}

const mdComponents = {
  // Headings
  h1: ({ children }: any) => <h1 className="text-base font-semibold mt-3 mb-1">{children}</h1>,
  h2: ({ children }: any) => <h2 className="text-sm font-semibold mt-2.5 mb-1">{children}</h2>,
  h3: ({ children }: any) => <h3 className="text-sm font-medium mt-2 mb-0.5">{children}</h3>,
  // Paragraphs
  p: ({ children }: any) => <p className="mb-2 last:mb-0 leading-relaxed">{children}</p>,
  // Lists
  ul: ({ children }: any) => <ul className="list-disc pl-5 mb-2 space-y-0.5">{children}</ul>,
  ol: ({ children }: any) => <ol className="list-decimal pl-5 mb-2 space-y-0.5">{children}</ol>,
  li: ({ children }: any) => <li className="leading-relaxed">{children}</li>,
  // Inline code
  code: ({ children, className }: any) => {
    const isBlock = className?.startsWith("language-");
    if (isBlock) return <code className={className}>{children}</code>;
    return (
      <code className="rounded bg-muted px-1 py-0.5 text-[11px] font-mono text-foreground">
        {children}
      </code>
    );
  },
  // Block code (non-SQL — SQL is handled separately above)
  pre: ({ children }: any) => (
    <pre className="rounded-lg bg-muted/80 border border-border/60 px-3.5 py-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap leading-relaxed my-2">
      {children}
    </pre>
  ),
  // Bold / italic
  strong: ({ children }: any) => <strong className="font-semibold">{children}</strong>,
  em: ({ children }: any) => <em className="italic">{children}</em>,
  // Blockquote
  blockquote: ({ children }: any) => (
    <blockquote className="border-l-2 border-border pl-3 text-muted-foreground my-2 italic">
      {children}
    </blockquote>
  ),
  // Horizontal rule
  hr: () => <hr className="border-border my-3" />,
  // Tables — rendered by remark-gfm; needs explicit styled components or they show as raw text
  table: ({ children }: any) => (
    <div className="overflow-x-auto my-2.5 rounded-lg border border-border/60 text-xs">
      <table className="w-full border-collapse">{children}</table>
    </div>
  ),
  thead: ({ children }: any) => <thead className="bg-muted/70 border-b border-border/60">{children}</thead>,
  tbody: ({ children }: any) => <tbody className="divide-y divide-border/30">{children}</tbody>,
  tr: ({ children }: any) => <tr className="hover:bg-muted/20 transition-colors">{children}</tr>,
  th: ({ children }: any) => (
    <th className="px-3 py-1.5 text-left font-medium text-muted-foreground whitespace-nowrap font-mono text-[11px]">
      {children}
    </th>
  ),
  td: ({ children }: any) => (
    <td className="px-3 py-1.5 text-foreground/90 font-mono text-[11px] max-w-[220px] truncate whitespace-nowrap">
      {children}
    </td>
  ),
};

export const MessageBubble = memo(function MessageBubble({
  role,
  content,
  streaming,
  catalog,
  schemaName,
  tool_steps,
  context_pruned,
  total_tokens,
  tool_count,
  onSuggestionClick,
  onRegenerate,
  isLast,
}: MessageBubbleProps) {
  const isUser = role === "user";

  return (
    <div className={cn("flex gap-3", isUser ? "flex-row-reverse" : "flex-row")}>
      {/* Avatar */}
      <div className={cn(
        "shrink-0 h-7 w-7 rounded-lg flex items-center justify-center mt-0.5",
        isUser ? "bg-primary text-primary-foreground" : "bg-primary/10 text-primary",
      )}>
        {isUser ? <User className="h-3.5 w-3.5" /> : <Sparkles className="h-3.5 w-3.5" />}
      </div>

      {/* Bubble */}
      <div className={cn(
        "max-w-[82%] rounded-2xl px-4 py-2.5 text-sm",
        isUser
          ? "bg-primary text-primary-foreground rounded-tr-sm"
          : "bg-muted/50 border border-border/60 text-foreground rounded-tl-sm",
      )}>
        {isUser ? (
          <span className="whitespace-pre-wrap break-words">{content}</span>
        ) : (
          <>
            {/* Tool call steps shown above the response text */}
            {tool_steps && tool_steps.length > 0 && (
              <div className="mb-1.5">
                {tool_steps.map((step) => (
                  <ToolCallStepView key={step.call_id} step={step} />
                ))}
              </div>
            )}
            {context_pruned && (
              <p className="text-[10px] text-muted-foreground/60 italic mb-1.5">
                Older context trimmed to fit context window.
              </p>
            )}
            {!content && !streaming ? (
              <span className="italic text-muted-foreground text-xs">
                No response — the model may be unavailable or returned an error.
              </span>
            ) : (
              splitOnCodeBlocks(content).map((part, i) =>
                part.type === "code" ? (
                  part.lang === "next-steps" ? (
                    // Render next-steps block as clickable suggestion chips
                    <div key={i} className="mt-3 flex flex-wrap gap-1.5">
                      {part.body.split("\n").map(s => s.trim()).filter(Boolean).map((s, j) => (
                        <button
                          key={j}
                          onClick={() => onSuggestionClick?.(s)}
                          className="text-[11px] rounded-full border border-primary/25 bg-primary/5 hover:bg-primary/12 hover:border-primary/50 px-3 py-1 text-foreground hover:text-primary transition-colors text-left"
                        >
                          {s}
                        </button>
                      ))}
                    </div>
                  ) : (
                  <div key={i} className="my-2.5 group/code relative">
                    <div className="absolute right-1.5 top-1.5 opacity-0 group-hover/code:opacity-100 transition-opacity">
                      <CopyButton text={part.body} className="rounded bg-background/80 border border-border/60 px-1.5 py-1" />
                    </div>
                    <pre className="rounded-lg bg-muted/80 border border-border/60 px-3.5 py-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap leading-relaxed">
                      <code>{part.body}</code>
                    </pre>
                    {(part.lang === "sql" || part.lang === "") && (
                      <SqlResultTable
                        sql={part.body}
                        catalog={catalog}
                        schemaName={schemaName}
                        onExplainResults={onSuggestionClick}
                      />
                    )}
                  </div>
                  )
                ) : (
                  <ReactMarkdown
                    key={i}
                    remarkPlugins={[remarkGfm]}
                    components={mdComponents as any}
                  >
                    {part.body}
                  </ReactMarkdown>
                )
              )
            )}
            {streaming && (
              <span className="inline-block h-4 w-0.5 ml-0.5 align-middle bg-current animate-pulse rounded-full" />
            )}
            {/* Action row: copy + regenerate (completed assistant messages only) */}
            {!streaming && content && (
              <div className="flex items-center gap-3 mt-2 pt-1.5 border-t border-border/40">
                <CopyButton text={stripBlocks(content)} label="Copy" />
                {isLast && onRegenerate && (
                  <button
                    type="button"
                    onClick={onRegenerate}
                    className="inline-flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors"
                    title="Regenerate response"
                  >
                    <RefreshCw className="h-3 w-3" />
                    Regenerate
                  </button>
                )}
                {(total_tokens || tool_count) ? (
                  <span className="text-[10px] text-muted-foreground/60 ml-auto">
                    {tool_count ? `${tool_count} tool${tool_count > 1 ? "s" : ""} · ` : ""}
                    {total_tokens ? `${total_tokens.toLocaleString()} tokens` : ""}
                  </span>
                ) : null}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
});
