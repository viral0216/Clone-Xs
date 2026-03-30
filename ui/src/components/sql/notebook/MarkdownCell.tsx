/**
 * MarkdownCell — Dual-mode markdown cell: edit (textarea) / render (rich HTML).
 */
import { useState } from "react";
import { Pencil, Eye } from "lucide-react";

interface Props {
  content: string;
  onChange: (content: string) => void;
}

// Simple inline markdown renderer (matches AiMarkdown pattern from SqlWorkbench)
function renderInline(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\([^)]+\))/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) return <strong key={i}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`")) return <code key={i} className="text-[#E8453C] bg-muted px-1 rounded text-[10px]">{part.slice(1, -1)}</code>;
    const linkMatch = part.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
    if (linkMatch) return <a key={i} href={linkMatch[2]} className="text-[#E8453C] underline" target="_blank" rel="noopener noreferrer">{linkMatch[1]}</a>;
    return part;
  });
}

function renderMarkdown(text: string): React.ReactNode {
  const lines = text.split("\n");
  return (
    <div className="space-y-1">
      {lines.map((line, i) => {
        const trimmed = line.trim();
        if (!trimmed) return <div key={i} className="h-1" />;
        if (trimmed.startsWith("### ")) return <h3 key={i} className="text-sm font-semibold text-foreground mt-2 mb-1">{renderInline(trimmed.slice(4))}</h3>;
        if (trimmed.startsWith("## ")) return <h2 key={i} className="text-base font-semibold text-foreground mt-3 mb-1">{renderInline(trimmed.slice(3))}</h2>;
        if (trimmed.startsWith("# ")) return <h1 key={i} className="text-lg font-bold text-foreground mt-3 mb-1">{renderInline(trimmed.slice(2))}</h1>;
        if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
          return (
            <div key={i} className="flex gap-2 pl-2">
              <span className="text-[#E8453C] shrink-0">•</span>
              <span className="text-xs">{renderInline(trimmed.slice(2))}</span>
            </div>
          );
        }
        if (/^\d+\.\s/.test(trimmed)) {
          const match = trimmed.match(/^(\d+)\.\s(.*)$/);
          return (
            <div key={i} className="flex gap-2 pl-2">
              <span className="text-muted-foreground shrink-0 text-xs">{match?.[1]}.</span>
              <span className="text-xs">{renderInline(match?.[2] || "")}</span>
            </div>
          );
        }
        if (trimmed.startsWith("---") || trimmed.startsWith("***")) return <hr key={i} className="border-border my-2" />;
        return <p key={i} className="text-xs text-foreground">{renderInline(trimmed)}</p>;
      })}
    </div>
  );
}

export default function MarkdownCell({ content, onChange }: Props) {
  const [editing, setEditing] = useState(!content);

  if (editing) {
    return (
      <div className="relative">
        <textarea
          value={content}
          onChange={e => onChange(e.target.value)}
          placeholder="Write markdown here... (## Heading, - bullet, **bold**, `code`)"
          className="w-full min-h-[60px] p-3 text-xs font-mono bg-background border-0 resize-y focus:outline-none placeholder:text-muted-foreground/40"
          onBlur={() => { if (content.trim()) setEditing(false); }}
          autoFocus
        />
        <button onClick={() => setEditing(false)} className="absolute top-1 right-1 p-1 text-muted-foreground hover:text-foreground" title="Preview">
          <Eye className="h-3 w-3" />
        </button>
      </div>
    );
  }

  return (
    <div className="relative px-3 py-2 cursor-pointer min-h-[40px] hover:bg-accent/10" onClick={() => setEditing(true)}>
      {content ? renderMarkdown(content) : <p className="text-xs text-muted-foreground italic">Empty markdown cell — click to edit</p>}
      <button onClick={(e) => { e.stopPropagation(); setEditing(true); }} className="absolute top-1 right-1 p-1 text-muted-foreground hover:text-foreground opacity-0 group-hover:opacity-100" title="Edit">
        <Pencil className="h-3 w-3" />
      </button>
    </div>
  );
}
