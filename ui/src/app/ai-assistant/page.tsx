// @ts-nocheck
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { Sparkles, Wand2, Database, Code, MessageSquare, Search } from "lucide-react";

export default function AiAssistantPage() {
  return (
    <div className="space-y-4">
      <PageHeader title="AI Assistant" icon={Sparkles} breadcrumbs={["Discovery", "AI Assistant"]}
        description="Ask questions about your data in natural language — AI generates SQL, executes it, and explains results." />

      <div className="flex items-center justify-center py-20">
        <div className="text-center max-w-lg">
          <div className="relative inline-block mb-6">
            <Sparkles className="h-16 w-16 text-muted-foreground/20" />
            <Badge className="absolute -top-2 -right-8 bg-[#E8453C] text-white text-[10px] px-2 py-0.5 shadow-lg">
              COMING SOON
            </Badge>
          </div>

          <h2 className="text-xl font-semibold mb-2">AI Assistant</h2>
          <p className="text-sm text-muted-foreground mb-6">
            Natural language to SQL powered by Databricks Model Serving endpoints.
            Ask questions about your data, get AI-generated queries, automatic execution, and plain-English explanations.
          </p>

          <div className="grid grid-cols-2 gap-3 text-left mb-6">
            {[
              { icon: Wand2, label: "AI Model & Genie", desc: "Use Databricks LLM or Genie spaces" },
              { icon: Database, label: "Auto Schema Discovery", desc: "Catalog and schema auto-populated" },
              { icon: Code, label: "SQL Generation", desc: "Natural language to Databricks SQL" },
              { icon: MessageSquare, label: "Multi-turn Chat", desc: "Follow-up questions with context" },
              { icon: Search, label: "Execute & Explain", desc: "Run queries and explain results" },
              { icon: Sparkles, label: "Smart Prompts", desc: "Databricks-aware SQL generation" },
            ].map(f => (
              <div key={f.label} className="flex items-start gap-2.5 p-3 rounded-lg bg-muted/30 border border-border">
                <f.icon className="h-4 w-4 text-muted-foreground mt-0.5 shrink-0" />
                <div>
                  <p className="text-xs font-medium">{f.label}</p>
                  <p className="text-[10px] text-muted-foreground">{f.desc}</p>
                </div>
              </div>
            ))}
          </div>

          <p className="text-[10px] text-muted-foreground">
            Configure your AI model in Settings → AI Model. Supports Claude, DBRX, Llama, Mixtral via Databricks serving endpoints.
          </p>
        </div>
      </div>
    </div>
  );
}
