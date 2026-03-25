import React, { StrictMode } from "react";
import ReactDOM, { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient } from "@tanstack/react-query";
import { PersistQueryClientProvider } from "@tanstack/react-query-persist-client";
import { createSyncStoragePersister } from "@tanstack/query-sync-storage-persister";
import App from "./App";
import { JobProvider } from "./contexts/JobContext";
import "./app/globals.css";

// WCAG AAA: Runtime accessibility checker (dev only)
if (import.meta.env.DEV) {
  import("@axe-core/react").then((axe) => {
    axe.default(React, ReactDOM, 1000);
  });
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30000,
      gcTime: 1000 * 60 * 60 * 24,  // 24 hours — keep cached data in memory
      refetchOnWindowFocus: false,
    },
  },
});

// Persist React Query cache to localStorage — survives page refreshes and browser restarts
const persister = createSyncStoragePersister({
  storage: window.localStorage,
  key: "clxs-query-cache",
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <PersistQueryClientProvider client={queryClient} persistOptions={{ persister, maxAge: 1000 * 60 * 60 * 24 }}>
      <JobProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </JobProvider>
    </PersistQueryClientProvider>
  </StrictMode>
);
