"use client";

import { useEffect, useRef } from "react";
import { MessageBubble } from "./MessageBubble";
import type { ChatMessage } from "../hooks/useChatStream";
import type { AgentMode } from "../hooks/useAgents";
import { ICON_MAP } from "./ChatInput";
import { Bot, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";

interface ChatThreadProps {
  messages: ChatMessage[];
  agents: AgentMode[];
  catalog?: string;
  schemaName?: string;
  activeMode?: string;
  onSuggestedPrompt?: (text: string) => void;
  onRegenerate?: () => void;
  streaming?: boolean;
}

export function ChatThread({ messages, agents, catalog, schemaName, activeMode = "assistant", onSuggestedPrompt, onRegenerate, streaming }: ChatThreadProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  if (!messages.length) {
    const agent   = agents.find((a) => a.value === activeMode) ?? agents[0];
    const Icon    = ICON_MAP[agent?.icon ?? ""] ?? Bot;
    const prompts = agent?.prompts ?? [];
    const modeLabel = agent?.label ?? activeMode.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

    return (
      <div className="flex-1 overflow-y-auto flex flex-col items-center justify-center px-6 py-8">
        <div className="w-full max-w-xl text-center">
          {/* Icon */}
          <div className="flex justify-center mb-4">
            <div className="h-14 w-14 rounded-2xl bg-primary/10 flex items-center justify-center">
              <Icon className={cn("h-7 w-7", agent?.color ?? "text-muted-foreground")} />
            </div>
          </div>

          <h3 className="text-base font-semibold mb-1">
            {activeMode === "assistant" ? "How can I help?" : modeLabel}
          </h3>
          <p className="text-xs text-muted-foreground mb-6 max-w-sm mx-auto leading-relaxed">
            {agent?.subtitle}
          </p>

          {/* Suggested prompts 2×2 grid */}
          <div className="grid grid-cols-2 gap-2">
            {prompts.map((p) => (
              <button
                key={p.label}
                onClick={() => onSuggestedPrompt?.(p.text)}
                className="text-left rounded-xl border border-border bg-muted/40 hover:bg-muted/70 hover:border-primary/30 transition-colors px-3.5 py-2.5 group"
              >
                <p className="text-xs font-medium text-foreground group-hover:text-primary transition-colors mb-0.5 leading-snug">{p.label}</p>
                <p className="text-[11px] text-muted-foreground leading-snug line-clamp-2">{p.text}</p>
              </button>
            ))}
          </div>

          {catalog && (
            <p className="mt-4 text-[11px] text-muted-foreground/70">
              <Sparkles className="h-3 w-3 inline mr-1 -mt-0.5" />
              Context: <span className="font-medium text-muted-foreground">{catalog}{schemaName ? `.${schemaName}` : ""}</span> will be injected
            </p>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
      {messages.map((msg, i) => (
        <MessageBubble
          key={i}
          role={msg.role}
          content={msg.content}
          streaming={msg.streaming}
          catalog={catalog}
          schemaName={schemaName}
          tool_steps={msg.tool_steps}
          context_pruned={msg.context_pruned}
          total_tokens={msg.total_tokens}
          tool_count={msg.tool_count}
          onSuggestionClick={onSuggestedPrompt}
          onRegenerate={onRegenerate}
          isLast={i === messages.length - 1 && msg.role === "assistant" && !streaming}
        />
      ))}
      <div ref={bottomRef} />
    </div>
  );
}
