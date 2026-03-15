/**
 * Typed API client for the Clone-X FastAPI backend.
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

  // Get credentials from sessionStorage
  const host = sessionStorage.getItem("dbx_host") || "";
  const token = sessionStorage.getItem("dbx_token") || "";

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(host && { "X-Databricks-Host": host }),
    ...(token && { "X-Databricks-Token": token }),
    ...(options.headers as Record<string, string>),
  };

  const res = await fetch(url, { ...fetchOpts, headers });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(error.detail || `API error: ${res.status}`);
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

  delete: <T>(path: string) =>
    apiFetch<T>(path, { method: "DELETE" }),
};
