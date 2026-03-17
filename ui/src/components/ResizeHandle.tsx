import { useCallback, useRef, useEffect } from "react";

interface ResizeHandleProps {
  /** Current width in pixels */
  width: number;
  /** Callback when width changes */
  onResize: (width: number) => void;
  /** Minimum width */
  min?: number;
  /** Maximum width */
  max?: number;
  /** Which side the handle sits on: "right" (panel on left) or "left" (panel on right) */
  side?: "right" | "left";
}

/**
 * Draggable resize handle for panels.
 * Place between a panel and its adjacent content.
 *
 * Usage:
 *   <div style={{ width: panelWidth }}>...panel...</div>
 *   <ResizeHandle width={panelWidth} onResize={setPanelWidth} min={180} max={500} side="right" />
 *   <div className="flex-1">...content...</div>
 */
export default function ResizeHandle({ width, onResize, min = 150, max = 600, side = "right" }: ResizeHandleProps) {
  const dragging = useRef(false);
  const startX = useRef(0);
  const startW = useRef(0);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    startX.current = e.clientX;
    startW.current = width;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  }, [width]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      const delta = side === "right"
        ? e.clientX - startX.current
        : startX.current - e.clientX;
      const newWidth = Math.min(max, Math.max(min, startW.current + delta));
      onResize(newWidth);
    };
    const handleMouseUp = () => {
      if (dragging.current) {
        dragging.current = false;
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      }
    };
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };
  }, [onResize, min, max, side]);

  return (
    <div
      className="shrink-0 w-1.5 cursor-col-resize group relative z-10 hover:bg-blue-600/20 active:bg-blue-600/30 transition-colors"
      onMouseDown={handleMouseDown}
      title="Drag to resize"
    >
      {/* Visual indicator — thin line that highlights on hover */}
      <div className="absolute inset-y-0 left-1/2 -translate-x-1/2 w-px bg-border group-hover:bg-blue-600 group-active:bg-blue-600 transition-colors" />
    </div>
  );
}
