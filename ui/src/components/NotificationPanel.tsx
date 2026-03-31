// @ts-nocheck
import { useState, useRef, useEffect } from "react";
import { useNotifications } from "@/hooks/useApi";
import { useQueryClient } from "@tanstack/react-query";
import { Bell, CheckCircle, XCircle, Info } from "lucide-react";

const typeIcon = {
  success: <CheckCircle className="h-3.5 w-3.5 text-foreground shrink-0" />,
  error: <XCircle className="h-3.5 w-3.5 text-red-500 shrink-0" />,
  info: <Info className="h-3.5 w-3.5 text-muted-foreground shrink-0" />,
};

function timeAgo(ts: string) {
  if (!ts) return "";
  const diff = Date.now() - new Date(ts).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

export default function NotificationPanel() {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const queryClient = useQueryClient();
  const { data } = useNotifications();

  const count = data?.unread_count ?? 0;
  const items = data?.items ?? [];

  function markAsRead() {
    localStorage.setItem("notifications_last_seen", new Date().toISOString());
    queryClient.invalidateQueries({ queryKey: ["notifications"] });
  }

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => { if (!open) markAsRead(); setOpen(!open); }}
        aria-label={count > 0 ? `${count} notifications` : "Notifications"}
        aria-expanded={open}
        aria-haspopup="true"
        className="relative p-1.5 rounded-md hover:bg-muted transition-colors"
      >
        <Bell className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
        {count > 0 && (
          <span className="absolute -top-0.5 -right-0.5 h-4 min-w-[16px] px-1 flex items-center justify-center text-[10px] font-bold text-white bg-red-500 rounded-full" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-2 w-80 bg-card border border-border rounded-lg shadow-lg z-50 overflow-hidden" role="region" aria-label="Notifications" aria-live="polite">
          <div className="px-4 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">Notifications</h3>
            <p className="text-xs text-muted-foreground">{items.length} recent events</p>
          </div>
          <div className="max-h-80 overflow-y-auto">
            {items.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                No notifications yet
              </div>
            ) : (
              items.map((item, i) => (
                <div
                  key={i}
                  className="flex items-start gap-2.5 px-4 py-3 border-b border-border last:border-0 hover:bg-muted/30 transition-colors"
                >
                  <div className="mt-0.5">{typeIcon[item.type] || typeIcon.info}</div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-foreground leading-snug">{item.message}</p>
                    <p className="text-xs text-muted-foreground mt-0.5">{timeAgo(item.timestamp)}</p>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
