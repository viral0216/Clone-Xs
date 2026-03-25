import React, { StrictMode } from "react";
import ReactDOM, { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
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
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <JobProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </JobProvider>
    </QueryClientProvider>
  </StrictMode>
);
