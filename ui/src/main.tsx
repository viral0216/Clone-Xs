import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import { JobProvider } from "./contexts/JobContext";
import "./app/globals.css";

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 30000 } },
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
