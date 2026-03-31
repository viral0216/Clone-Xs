/**
 * NotebookSearchBar — Find across all cells with match count and navigation.
 */
import { useState, useEffect, useRef } from "react";
import { X, ChevronUp, ChevronDown, Search } from "lucide-react";

interface CellMatch {
  cellId: string;
  cellIndex: number;
  count: number;
}

interface Props {
  cells: { id: string; content: string }[];
  onClose: () => void;
  onNavigate: (cellId: string) => void;
  searchQuery: string;
  onSearchChange: (query: string) => void;
}

export default function NotebookSearchBar({ cells, onClose, onNavigate, searchQuery, onSearchChange }: Props) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [currentIdx, setCurrentIdx] = useState(0);

  // Compute matches
  const matches: CellMatch[] = [];
  let totalMatches = 0;
  if (searchQuery.trim()) {
    const lower = searchQuery.toLowerCase();
    cells.forEach((cell, i) => {
      const count = (cell.content.toLowerCase().split(lower).length - 1);
      if (count > 0) {
        matches.push({ cellId: cell.id, cellIndex: i, count });
        totalMatches += count;
      }
    });
  }

  useEffect(() => { inputRef.current?.focus(); }, []);
  useEffect(() => { setCurrentIdx(0); }, [searchQuery]);

  function goNext() {
    if (matches.length === 0) return;
    const next = (currentIdx + 1) % matches.length;
    setCurrentIdx(next);
    onNavigate(matches[next].cellId);
  }

  function goPrev() {
    if (matches.length === 0) return;
    const prev = (currentIdx - 1 + matches.length) % matches.length;
    setCurrentIdx(prev);
    onNavigate(matches[prev].cellId);
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Escape") onClose();
    if (e.key === "Enter") { e.shiftKey ? goPrev() : goNext(); }
  }

  return (
    <div className="flex items-center gap-2 px-4 py-1.5 border-b border-border bg-muted/30 shrink-0">
      <Search className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
      <input
        ref={inputRef}
        value={searchQuery}
        onChange={e => onSearchChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Find in notebook..."
        className="flex-1 text-xs bg-transparent border-0 focus:outline-none"
      />
      {searchQuery.trim() && (
        <span className="text-[10px] text-muted-foreground shrink-0">
          {totalMatches > 0 ? `${currentIdx + 1} of ${matches.length} cells (${totalMatches} matches)` : "No matches"}
        </span>
      )}
      <button onClick={goPrev} disabled={matches.length === 0} className="text-muted-foreground hover:text-foreground disabled:opacity-30" title="Previous (Shift+Enter)">
        <ChevronUp className="h-3.5 w-3.5" />
      </button>
      <button onClick={goNext} disabled={matches.length === 0} className="text-muted-foreground hover:text-foreground disabled:opacity-30" title="Next (Enter)">
        <ChevronDown className="h-3.5 w-3.5" />
      </button>
      <button onClick={onClose} className="text-muted-foreground hover:text-foreground" title="Close (Esc)">
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
