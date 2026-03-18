/**
 * Typed API client for the Clone-Xs FastAPI backend.
 */

// Use Vite proxy (/api -> localhost:8000/api) — no CORS issues, no timeout
const API_BASE = "/api";

interface FetchOptions extends RequestInit {
  params?: Record<string, string>;
}

async function apiFetch<T>(path: string, options: FetchOptions = {}): Promise<T> {
  const { params, ...fetchOpts } = options;
  let url = `${API_BASE}${path}`;

  if (params) {
    const searchParams = new URLSearchParams(params);
    url += `?${searchParams.toString()}`;
  }

  // Get credentials from sessionStorage, warehouse from localStorage (persists across sessions)
  const host = sessionStorage.getItem("dbx_host") || "";
  const token = sessionStorage.getItem("dbx_token") || "";
  const warehouse = localStorage.getItem("dbx_warehouse_id") || "";

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
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
