/**
 * Typed API client for the Clone-Xs FastAPI backend.
 */

import { toast } from "sonner";
import { getMockResponse } from "./mock-data";

// Use Vite proxy (/api -> localhost:8000/api) — no CORS issues, no timeout
const API_BASE = "/api";

// Track shown toasts to avoid spamming the same message
let _lastToast = "";
let _lastToastTime = 0;

interface FetchOptions extends RequestInit {
  params?: Record<string, string>;
}

async function apiFetch<T>(path: string, options: FetchOptions = {}): Promise<T> {
  // Demo mode: return mock data without hitting backend
  if (sessionStorage.getItem("demo_mode") === "true") {
    const body = options.body ? JSON.parse(options.body as string) : undefined;
    const mock = getMockResponse(path, body);
    if (mock !== undefined) return mock as T;
  }

  const { params, ...fetchOpts } = options;
  let url = `${API_BASE}${path}`;

  if (params) {
    const searchParams = new URLSearchParams(params);
    url += `?${searchParams.toString()}`;
  }

  // Credentials in localStorage
  const host = localStorage.getItem("dbx_host") || "";
  const token = localStorage.getItem("dbx_token") || "";
  const warehouse = localStorage.getItem("dbx_warehouse_id") || "";
  const sessionId = localStorage.getItem("clxs_session_id") || "";

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(sessionId && { "X-Clone-Session": sessionId }),
    ...(host && { "X-Databricks-Host": host }),
    ...(token && { "X-Databricks-Token": token }),
    ...(warehouse && { "X-Databricks-Warehouse": warehouse }),
    ...(options.headers as Record<string, string>),
  };

  const res = await fetch(url, { ...fetchOpts, headers });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    let message = `API error: ${res.status}`;
    if (typeof error.detail === "string") {
      message = error.detail;
    } else if (Array.isArray(error.detail)) {
      message = error.detail.map((e: any) => e.msg || JSON.stringify(e)).join("; ");
    } else if (error.detail) {
      message = JSON.stringify(error.detail);
    }
    // Show toast for actionable errors (debounced to avoid spam)
    const now = Date.now();
    const msgLower = message.toLowerCase();
    const isActionable =
      msgLower.includes("no sql warehouse") ||
      msgLower.includes("warehouse") && (msgLower.includes("not found") || msgLower.includes("not a valid")) ||
      msgLower.includes("session expired") ||
      msgLower.includes("not authenticated") ||
      msgLower.includes("please log in");

    if (isActionable && (message !== _lastToast || now - _lastToastTime > 5000)) {
      _lastToast = message;
      _lastToastTime = now;
      toast.error(message);
    }

    throw new Error(message);
  }

  return res.json();
}

export const api = {
  get: <T>(path: string, params?: Record<string, string>) =>
    apiFetch<T>(path, { method: "GET", params }),

  post: <T>(path: string, body?: unknown) =>
    apiFetch<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined }),

  put: <T>(path: string, body?: unknown) =>
    apiFetch<T>(path, { method: "PUT", body: body ? JSON.stringify(body) : undefined }),

  patch: <T>(path: string, body?: unknown) =>
    apiFetch<T>(path, { method: "PATCH", body: body ? JSON.stringify(body) : undefined }),

  delete: <T>(path: string) =>
    apiFetch<T>(path, { method: "DELETE" }),
};
