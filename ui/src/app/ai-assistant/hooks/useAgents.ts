"use client";

import { useEffect, useState } from "react";

export interface AgentPrompt {
  label: string;
  text: string;
}

export interface AgentMode {
  value: string;
  label: string;
  subtitle: string;
  icon: string;
  color: string;
  order: number;
  prompts: AgentPrompt[];
}

export function useAgents(): AgentMode[] {
  const [agents, setAgents] = useState<AgentMode[]>([]);

  useEffect(() => {
    fetch("/api/ai-assistant/agents")
      .then((r) => r.json())
      .then((data: AgentMode[]) => setAgents(data))
      .catch(() => {});
  }, []);

  return agents;
}
