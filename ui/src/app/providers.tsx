import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import { ActiveJobsProvider } from "@/contexts/ActiveJobsContext";

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(
    () => new QueryClient({ defaultOptions: { queries: { staleTime: 30000 } } })
  );
  return (
    <QueryClientProvider client={queryClient}>
      <ActiveJobsProvider>
        {children}
      </ActiveJobsProvider>
    </QueryClientProvider>
  );
}
