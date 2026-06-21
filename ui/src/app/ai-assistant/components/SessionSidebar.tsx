"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api-client";
import { MessageSquarePlus, Pin, PinOff, Trash2, Pencil, Check, X } from "lucide-react";

interface SessionMeta {
  id: string;
  title: string;
  pinned: boolean;
  updated: number;
  message_count: number;
}

interface SessionSidebarProps {
  activeSessionId: string | null;
  onSelect: (session: { id: string; messages: { role: "user" | "assistant"; content: string }[] }) => void;
  onNew: () => void;
  refreshTrigger?: number;
}

function timeLabel(ts: number): string {
  const diff = (Date.now() / 1000) - ts;
  if (diff < 60)       return "just now";
  if (diff < 3600)     return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400)    return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 86400 * 7) return `${Math.floor(diff / 86400)}d ago`;
  return new Date(ts * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function groupByDate(sessions: SessionMeta[]): { label: string; items: SessionMeta[] }[] {
  const now = Date.now() / 1000;
  const pinned = sessions.filter((s) => s.pinned);
  const rest   = sessions.filter((s) => !s.pinned);

  const groups: { label: string; items: SessionMeta[] }[] = [];
  if (pinned.length) groups.push({ label: "Pinned", items: pinned });

  const buckets: Record<string, SessionMeta[]> = { Today: [], Yesterday: [], "This week": [], Older: [] };
  for (const s of rest) {
    const diff = now - s.updated;
    if (diff < 86400)       buckets["Today"].push(s);
    else if (diff < 172800) buckets["Yesterday"].push(s);
    else if (diff < 604800) buckets["This week"].push(s);
    else                    buckets["Older"].push(s);
  }
  for (const [label, items] of Object.entries(buckets)) {
    if (items.length) groups.push({ label, items });
  }
  return groups;
}

export function SessionSidebar({ activeSessionId, onSelect, onNew, refreshTrigger }: SessionSidebarProps) {
  const [sessions, setSessions]   = useState<SessionMeta[]>([]);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editTitle, setEditTitle] = useState("");
  const editInputRef = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    try {
      const data = await api.get<SessionMeta[]>("/ai-assistant/sessions");
      setSessions(data || []);
    } catch {}
  }, []);

  useEffect(() => { load(); }, [load, refreshTrigger]);
  useEffect(() => { if (editingId) editInputRef.current?.focus(); }, [editingId]);

  const openSession = async (id: string) => {
    try {
      const data = await api.get<{ id: string; messages: { role: "user" | "assistant"; content: string }[] }>(`/ai-assistant/sessions/${id}`);
      onSelect(data);
    } catch {}
  };

  const deleteSession = async (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    await api.delete(`/ai-assistant/sessions/${id}`);
    setSessions((prev) => prev.filter((s) => s.id !== id));
  };

  const togglePin = async (e: React.MouseEvent, s: SessionMeta) => {
    e.stopPropagation();
    await api.post(`/ai-assistant/sessions/${s.id}/pin`, { pinned: !s.pinned });
    load();
  };

  const startEdit = (e: React.MouseEvent, s: SessionMeta) => {
    e.stopPropagation();
    setEditingId(s.id);
    setEditTitle(s.title);
  };

  const commitEdit = async (id: string) => {
    if (editTitle.trim()) {
      await api.post(`/ai-assistant/sessions/${id}/rename`, { title: editTitle.trim() });
      load();
    }
    setEditingId(null);
  };

  const groups = groupByDate(sessions);

  return (
    <div className="flex flex-col h-full bg-muted/20">
      {/* Header */}
      <div className="px-3 py-3 border-b border-border">
        <Button
          variant="outline"
          size="sm"
          className="w-full h-8 text-xs gap-2 justify-start font-normal hover:bg-primary hover:text-primary-foreground hover:border-primary transition-colors"
          onClick={onNew}
        >
          <MessageSquarePlus className="h-3.5 w-3.5" />
          New Chat
        </Button>
      </div>

      <ScrollArea className="flex-1">
        <div className="py-2">
          {sessions.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-10 px-4 text-center gap-2">
              <div className="h-10 w-10 rounded-xl bg-muted flex items-center justify-center">
                <MessageSquarePlus className="h-5 w-5 text-muted-foreground/50" />
              </div>
              <p className="text-xs text-muted-foreground">No conversations yet.</p>
              <p className="text-[10px] text-muted-foreground/60">Start a new chat above.</p>
            </div>
          ) : (
            groups.map((group) => (
              <div key={group.label} className="mb-1">
                <p className="px-3 py-1 text-[10px] font-semibold text-muted-foreground/60 uppercase tracking-wider">
                  {group.label}
                </p>
                {group.items.map((s) => (
                  <SessionRow
                    key={s.id}
                    session={s}
                    active={activeSessionId === s.id}
                    editing={editingId === s.id}
                    editTitle={editTitle}
                    editInputRef={editInputRef}
                    onOpen={() => openSession(s.id)}
                    onDelete={(e) => deleteSession(e, s.id)}
                    onTogglePin={(e) => togglePin(e, s)}
                    onStartEdit={(e) => startEdit(e, s)}
                    onEditChange={setEditTitle}
                    onCommitEdit={() => commitEdit(s.id)}
                    onCancelEdit={() => setEditingId(null)}
                  />
                ))}
              </div>
            ))
          )}
        </div>
      </ScrollArea>
    </div>
  );
}

interface SessionRowProps {
  session: SessionMeta;
  active: boolean;
  editing: boolean;
  editTitle: string;
  editInputRef: React.RefObject<HTMLInputElement | null>;
  onOpen: () => void;
  onDelete: (e: React.MouseEvent) => void;
  onTogglePin: (e: React.MouseEvent) => void;
  onStartEdit: (e: React.MouseEvent) => void;
  onEditChange: (v: string) => void;
  onCommitEdit: () => void;
  onCancelEdit: () => void;
}

function SessionRow({
  session, active, editing, editTitle, editInputRef,
  onOpen, onDelete, onTogglePin, onStartEdit, onEditChange, onCommitEdit, onCancelEdit,
}: SessionRowProps) {
  return (
    <div
      onClick={onOpen}
      className={cn(
        "group mx-2 flex items-start gap-1 rounded-lg px-2 py-1.5 cursor-pointer transition-colors",
        active ? "bg-primary/10 text-primary" : "hover:bg-muted/60",
      )}
    >
      <div className="flex-1 min-w-0">
        {editing ? (
          <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
            <Input
              ref={editInputRef}
              value={editTitle}
              onChange={(e) => onEditChange(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter")  onCommitEdit();
                if (e.key === "Escape") onCancelEdit();
              }}
              className="h-5 text-[11px] px-1 py-0"
            />
            <button onClick={onCommitEdit} className="shrink-0 text-muted-foreground hover:text-foreground">
              <Check className="h-3 w-3" />
            </button>
            <button onClick={onCancelEdit} className="shrink-0 text-muted-foreground hover:text-foreground">
              <X className="h-3 w-3" />
            </button>
          </div>
        ) : (
          <>
            <p className={cn("text-[11px] font-medium truncate leading-snug", active && "text-primary")}>
              {session.title}
            </p>
            <p className="text-[10px] text-muted-foreground">
              {timeLabel(session.updated)} · {session.message_count} msg{session.message_count !== 1 ? "s" : ""}
            </p>
          </>
        )}
      </div>

      {!editing && (
        <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity shrink-0 mt-0.5">
          <ActionBtn onClick={onStartEdit}  title="Rename" icon={<Pencil  className="h-3 w-3" />} />
          <ActionBtn onClick={onTogglePin}  title={session.pinned ? "Unpin" : "Pin"} icon={session.pinned ? <PinOff className="h-3 w-3" /> : <Pin className="h-3 w-3" />} />
          <ActionBtn onClick={onDelete}     title="Delete" icon={<Trash2  className="h-3 w-3" />} danger />
        </div>
      )}
    </div>
  );
}

function ActionBtn({ onClick, title, icon, danger }: { onClick: (e: React.MouseEvent) => void; title: string; icon: React.ReactNode; danger?: boolean }) {
  return (
    <button
      onClick={onClick}
      title={title}
      className={cn(
        "p-0.5 rounded text-muted-foreground transition-colors",
        danger ? "hover:text-destructive" : "hover:text-foreground",
      )}
    >
      {icon}
    </button>
  );
}
