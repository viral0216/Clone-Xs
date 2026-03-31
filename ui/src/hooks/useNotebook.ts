/**
 * useNotebook — Reducer-based state management for SQL Notebooks.
 */
import { useReducer, useCallback, useEffect, useRef } from "react";

export interface NotebookCell {
  id: string;
  type: "sql" | "markdown";
  content: string;
  // Runtime state (not persisted)
  results?: any[] | null;
  error?: string | null;
  elapsed?: number | null;
  running?: boolean;
  viewMode?: "table" | "chart";
  chartType?: string;
  chartXCol?: string;
  chartYCol?: string;
  collapsed?: boolean;
  executionCount?: number | null; // Jupyter-style [1], [2], [*]
  lastRunAt?: number | null;
}

export interface NotebookParams {
  [key: string]: string;
}

interface CellSnapshot { id: string; type: "sql" | "markdown"; content: string }

export interface NotebookState {
  id: string | null;
  title: string;
  cells: NotebookCell[];
  dirty: boolean;
  runningAll: boolean;
  executionCounter: number;
  params: NotebookParams;
  focusedCellId: string | null;
  // Undo/redo
  undoStack: CellSnapshot[][];
  redoStack: CellSnapshot[][];
}

type Action =
  | { type: "SET_NOTEBOOK"; payload: { id: string; title: string; cells: NotebookCell[] } }
  | { type: "NEW_NOTEBOOK" }
  | { type: "ADD_CELL"; payload: { afterId?: string; cellType: "sql" | "markdown" } }
  | { type: "DELETE_CELL"; payload: { id: string } }
  | { type: "DUPLICATE_CELL"; payload: { id: string } }
  | { type: "MOVE_CELL"; payload: { id: string; direction: "up" | "down" } }
  | { type: "REORDER_CELLS"; payload: { fromIndex: number; toIndex: number } }
  | { type: "UPDATE_CELL"; payload: { id: string; patch: Partial<NotebookCell> } }
  | { type: "SET_TITLE"; payload: string }
  | { type: "SET_DIRTY"; payload: boolean }
  | { type: "SET_RUNNING_ALL"; payload: boolean }
  | { type: "SET_ID"; payload: string }
  | { type: "INCREMENT_EXECUTION"; payload: { cellId: string } }
  | { type: "SET_PARAMS"; payload: NotebookParams }
  | { type: "SET_FOCUSED_CELL"; payload: string | null }
  | { type: "TOGGLE_COLLAPSE"; payload: { id: string } }
  | { type: "IMPORT_CELLS"; payload: NotebookCell[] }
  | { type: "UNDO" }
  | { type: "REDO" }
  | { type: "PUSH_UNDO" };

function uid(): string {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

function makeCell(cellType: "sql" | "markdown"): NotebookCell {
  return { id: uid(), type: cellType, content: "", collapsed: false, executionCount: null };
}

const initialState: NotebookState = {
  id: null,
  title: "Untitled Notebook",
  cells: [makeCell("sql")],
  dirty: false,
  runningAll: false,
  executionCounter: 0,
  params: {},
  focusedCellId: null,
  undoStack: [],
  redoStack: [],
};

function reducer(state: NotebookState, action: Action): NotebookState {
  switch (action.type) {
    case "SET_NOTEBOOK":
      return { ...state, id: action.payload.id, title: action.payload.title, cells: action.payload.cells, dirty: false, executionCounter: 0 };

    case "NEW_NOTEBOOK":
      return { ...initialState, cells: [makeCell("sql")] };

    case "ADD_CELL": {
      const newCell = makeCell(action.payload.cellType);
      if (!action.payload.afterId) {
        return { ...state, cells: [...state.cells, newCell], dirty: true, focusedCellId: newCell.id };
      }
      const idx = state.cells.findIndex(c => c.id === action.payload.afterId);
      const cells = [...state.cells];
      cells.splice(idx + 1, 0, newCell);
      return { ...state, cells, dirty: true, focusedCellId: newCell.id };
    }

    case "DELETE_CELL": {
      if (state.cells.length <= 1) return state;
      const idx = state.cells.findIndex(c => c.id === action.payload.id);
      const newCells = state.cells.filter(c => c.id !== action.payload.id);
      const nextFocus = newCells[Math.min(idx, newCells.length - 1)]?.id || null;
      return { ...state, cells: newCells, dirty: true, focusedCellId: nextFocus };
    }

    case "DUPLICATE_CELL": {
      const idx = state.cells.findIndex(c => c.id === action.payload.id);
      if (idx < 0) return state;
      const source = state.cells[idx];
      const clone: NotebookCell = { ...makeCell(source.type), content: source.content };
      const cells = [...state.cells];
      cells.splice(idx + 1, 0, clone);
      return { ...state, cells, dirty: true, focusedCellId: clone.id };
    }

    case "MOVE_CELL": {
      const idx = state.cells.findIndex(c => c.id === action.payload.id);
      if (idx < 0) return state;
      const newIdx = action.payload.direction === "up" ? idx - 1 : idx + 1;
      if (newIdx < 0 || newIdx >= state.cells.length) return state;
      const cells = [...state.cells];
      [cells[idx], cells[newIdx]] = [cells[newIdx], cells[idx]];
      return { ...state, cells, dirty: true };
    }

    case "REORDER_CELLS": {
      const { fromIndex, toIndex } = action.payload;
      if (fromIndex === toIndex) return state;
      const cells = [...state.cells];
      const [moved] = cells.splice(fromIndex, 1);
      cells.splice(toIndex, 0, moved);
      return { ...state, cells, dirty: true };
    }

    case "UPDATE_CELL":
      return {
        ...state,
        cells: state.cells.map(c => c.id === action.payload.id ? { ...c, ...action.payload.patch } : c),
        dirty: true,
      };

    case "SET_TITLE":
      return { ...state, title: action.payload, dirty: true };

    case "SET_DIRTY":
      return { ...state, dirty: action.payload };

    case "SET_RUNNING_ALL":
      return { ...state, runningAll: action.payload };

    case "SET_ID":
      return { ...state, id: action.payload };

    case "INCREMENT_EXECUTION": {
      const next = state.executionCounter + 1;
      return {
        ...state,
        executionCounter: next,
        cells: state.cells.map(c => c.id === action.payload.cellId ? { ...c, executionCount: next, lastRunAt: Date.now() } : c),
      };
    }

    case "SET_PARAMS":
      return { ...state, params: action.payload };

    case "SET_FOCUSED_CELL":
      return { ...state, focusedCellId: action.payload };

    case "TOGGLE_COLLAPSE":
      return {
        ...state,
        cells: state.cells.map(c => c.id === action.payload.id ? { ...c, collapsed: !c.collapsed } : c),
      };

    case "IMPORT_CELLS":
      return { ...state, cells: [...state.cells, ...action.payload], dirty: true };

    case "PUSH_UNDO": {
      const snapshot = state.cells.map(c => ({ id: c.id, type: c.type, content: c.content }));
      const stack = [...state.undoStack, snapshot].slice(-50); // cap at 50
      return { ...state, undoStack: stack, redoStack: [] };
    }

    case "UNDO": {
      if (state.undoStack.length === 0) return state;
      const prev = state.undoStack[state.undoStack.length - 1];
      const currentSnap = state.cells.map(c => ({ id: c.id, type: c.type, content: c.content }));
      return {
        ...state,
        cells: prev.map(s => ({ ...makeCell(s.type), id: s.id, content: s.content })),
        undoStack: state.undoStack.slice(0, -1),
        redoStack: [...state.redoStack, currentSnap],
        dirty: true,
      };
    }

    case "REDO": {
      if (state.redoStack.length === 0) return state;
      const next = state.redoStack[state.redoStack.length - 1];
      const currentSnap = state.cells.map(c => ({ id: c.id, type: c.type, content: c.content }));
      return {
        ...state,
        cells: next.map(s => ({ ...makeCell(s.type), id: s.id, content: s.content })),
        redoStack: state.redoStack.slice(0, -1),
        undoStack: [...state.undoStack, currentSnap],
        dirty: true,
      };
    }

    default:
      return state;
  }
}

// localStorage persistence
const STORAGE_KEY = "clxs-notebooks";

export interface SavedNotebook {
  id: string;
  title: string;
  cells: { id: string; type: "sql" | "markdown"; content: string }[];
  createdAt: string;
  updatedAt: string;
}

export function loadNotebooks(): SavedNotebook[] {
  try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); } catch { return []; }
}

export function persistNotebooks(notebooks: SavedNotebook[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(notebooks));
}

export function saveNotebook(state: NotebookState): string {
  const notebooks = loadNotebooks();
  const now = new Date().toISOString();
  const cells = state.cells.map(c => ({ id: c.id, type: c.type, content: c.content }));

  if (state.id) {
    const idx = notebooks.findIndex(n => n.id === state.id);
    if (idx >= 0) {
      notebooks[idx] = { ...notebooks[idx], title: state.title, cells, updatedAt: now };
    } else {
      notebooks.push({ id: state.id, title: state.title, cells, createdAt: now, updatedAt: now });
    }
    persistNotebooks(notebooks);
    return state.id;
  }

  const id = uid();
  notebooks.push({ id, title: state.title, cells, createdAt: now, updatedAt: now });
  persistNotebooks(notebooks);
  return id;
}

export function deleteNotebook(id: string) {
  const notebooks = loadNotebooks().filter(n => n.id !== id);
  persistNotebooks(notebooks);
}

/** Extract {{variable}} placeholders from all SQL cells */
export function extractParams(cells: NotebookCell[]): string[] {
  const params = new Set<string>();
  for (const cell of cells) {
    if (cell.type !== "sql") continue;
    const matches = cell.content.matchAll(/\{\{(\w+)\}\}/g);
    for (const m of matches) params.add(m[1]);
  }
  return [...params].sort();
}

/** Replace {{variable}} placeholders in SQL with param values */
export function interpolateParams(sql: string, params: NotebookParams): string {
  return sql.replace(/\{\{(\w+)\}\}/g, (_, key) => params[key] || `{{${key}}}`);
}

/** Extract headings from markdown cells for table of contents */
export function extractToc(cells: NotebookCell[]): { cellId: string; level: number; text: string }[] {
  const toc: { cellId: string; level: number; text: string }[] = [];
  for (const cell of cells) {
    if (cell.type !== "markdown") continue;
    for (const line of cell.content.split("\n")) {
      const match = line.match(/^(#{1,3})\s+(.+)$/);
      if (match) {
        toc.push({ cellId: cell.id, level: match[1].length, text: match[2].trim() });
      }
    }
  }
  return toc;
}

export function useNotebook() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const autoSaveTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  // Auto-save every 30 seconds if dirty
  useEffect(() => {
    autoSaveTimer.current = setInterval(() => {
      if (state.dirty && state.cells.some(c => c.content.trim())) {
        const id = saveNotebook(state);
        dispatch({ type: "SET_ID", payload: id });
        dispatch({ type: "SET_DIRTY", payload: false });
      }
    }, 30000);
    return () => { if (autoSaveTimer.current) clearInterval(autoSaveTimer.current); };
  }, [state.dirty, state.cells, state.title, state.id]);

  const pushUndo = useCallback(() => {
    dispatch({ type: "PUSH_UNDO" });
  }, []);

  const undo = useCallback(() => {
    dispatch({ type: "UNDO" });
  }, []);

  const redo = useCallback(() => {
    dispatch({ type: "REDO" });
  }, []);

  const addCell = useCallback((cellType: "sql" | "markdown", afterId?: string) => {
    dispatch({ type: "PUSH_UNDO" });
    dispatch({ type: "ADD_CELL", payload: { cellType, afterId } });
  }, []);

  const deleteCell = useCallback((id: string) => {
    dispatch({ type: "PUSH_UNDO" });
    dispatch({ type: "DELETE_CELL", payload: { id } });
  }, []);

  const duplicateCell = useCallback((id: string) => {
    dispatch({ type: "PUSH_UNDO" });
    dispatch({ type: "DUPLICATE_CELL", payload: { id } });
  }, []);

  const moveCell = useCallback((id: string, direction: "up" | "down") => {
    dispatch({ type: "PUSH_UNDO" });
    dispatch({ type: "MOVE_CELL", payload: { id, direction } });
  }, []);

  const updateCell = useCallback((id: string, patch: Partial<NotebookCell>) => {
    dispatch({ type: "UPDATE_CELL", payload: { id, patch } });
  }, []);

  const setTitle = useCallback((title: string) => {
    dispatch({ type: "SET_TITLE", payload: title });
  }, []);

  const loadNotebook = useCallback((saved: SavedNotebook) => {
    dispatch({ type: "SET_NOTEBOOK", payload: { id: saved.id, title: saved.title, cells: saved.cells.map(c => ({ ...c })) } });
  }, []);

  const newNotebook = useCallback(() => {
    dispatch({ type: "NEW_NOTEBOOK" });
  }, []);

  const save = useCallback(() => {
    const id = saveNotebook(state);
    dispatch({ type: "SET_ID", payload: id });
    dispatch({ type: "SET_DIRTY", payload: false });
    return id;
  }, [state]);

  const setRunningAll = useCallback((val: boolean) => {
    dispatch({ type: "SET_RUNNING_ALL", payload: val });
  }, []);

  const incrementExecution = useCallback((cellId: string) => {
    dispatch({ type: "INCREMENT_EXECUTION", payload: { cellId } });
  }, []);

  const setParams = useCallback((params: NotebookParams) => {
    dispatch({ type: "SET_PARAMS", payload: params });
  }, []);

  const setFocusedCell = useCallback((id: string | null) => {
    dispatch({ type: "SET_FOCUSED_CELL", payload: id });
  }, []);

  const toggleCollapse = useCallback((id: string) => {
    dispatch({ type: "TOGGLE_COLLAPSE", payload: { id } });
  }, []);

  const reorderCells = useCallback((fromIndex: number, toIndex: number) => {
    dispatch({ type: "REORDER_CELLS", payload: { fromIndex, toIndex } });
  }, []);

  const importCells = useCallback((cells: NotebookCell[]) => {
    dispatch({ type: "IMPORT_CELLS", payload: cells });
  }, []);

  return {
    state,
    addCell,
    deleteCell,
    duplicateCell,
    moveCell,
    updateCell,
    setTitle,
    loadNotebook,
    newNotebook,
    save,
    setRunningAll,
    incrementExecution,
    setParams,
    setFocusedCell,
    toggleCollapse,
    reorderCells,
    importCells,
    undo,
    redo,
  };
}
