// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useAzureCosts, useCostEstimate } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import {
  Target, Loader2, Plus, Trash2, AlertTriangle, CheckCircle,
  DollarSign, TrendingUp, PiggyBank,
} from "lucide-react";

const STORAGE_KEY = "clxs-finops-budgets";

interface Budget {
  id: string;
  name: string;
  amount: number;
  period: "monthly" | "quarterly" | "yearly";
  category: "total" | "databricks" | "storage" | "compute";
  created_at: string;
}

function loadBudgets(): Budget[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveBudgets(budgets: Budget[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(budgets));
}

function progressColor(pct: number) {
  if (pct >= 100) return "bg-red-500";
  if (pct >= 80) return "bg-amber-500";
  return "bg-green-500";
}

function progressTextColor(pct: number) {
  if (pct >= 100) return "text-red-500";
  if (pct >= 80) return "text-amber-500";
  return "text-green-500";
}

function SummaryCard({ label, value, color, icon: Icon }: { label: string; value: string | number; color?: string; icon?: any }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <div className="flex items-center justify-between">
          <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
          {Icon && <Icon className={`h-4 w-4 ${colorClass || "text-muted-foreground"}`} />}
        </div>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  );
}

export default function BudgetTrackerPage() {
  const [budgets, setBudgets] = useState<Budget[]>([]);

  const catalogForEstimate = (typeof window !== "undefined" ? localStorage.getItem("clxs-last-catalog") : "") || "";
  const azureQuery = useAzureCosts(30);
  const estimateQuery = useCostEstimate(catalogForEstimate);

  const loading = azureQuery.isLoading || estimateQuery.isLoading;

  const actualSpend: Record<string, number> = (() => {
    const spend: Record<string, number> = { total: 0, databricks: 0, storage: 0, compute: 0 };
    if (azureQuery.data) {
      const data = azureQuery.data as any;
      spend.total = data.total_cost || 0;
      spend.databricks = data.databricks_costs?.total || 0;
      spend.compute = (data.total_cost || 0) - (data.databricks_costs?.total || 0);
    }
    if (estimateQuery.data) {
      const data = estimateQuery.data as any;
      spend.storage = data.monthly_cost_usd || 0;
    }
    if (spend.total === 0 && spend.storage > 0) {
      spend.total = spend.databricks + spend.storage + spend.compute;
    }
    return spend;
  })();

  // Form state
  const [showForm, setShowForm] = useState(false);
  const [formName, setFormName] = useState("");
  const [formAmount, setFormAmount] = useState("");
  const [formPeriod, setFormPeriod] = useState<"monthly" | "quarterly" | "yearly">("monthly");
  const [formCategory, setFormCategory] = useState<"total" | "databricks" | "storage" | "compute">("total");

  useEffect(() => {
    setBudgets(loadBudgets());
  }, []);

  function addBudget() {
    if (!formName.trim() || !formAmount) {
      toast.error("Budget name and amount are required");
      return;
    }
    const newBudget: Budget = {
      id: crypto.randomUUID(),
      name: formName.trim(),
      amount: parseFloat(formAmount),
      period: formPeriod,
      category: formCategory,
      created_at: new Date().toISOString(),
    };
    const updated = [...budgets, newBudget];
    setBudgets(updated);
    saveBudgets(updated);
    setFormName("");
    setFormAmount("");
    setFormPeriod("monthly");
    setFormCategory("total");
    setShowForm(false);
    toast.success(`Budget "${newBudget.name}" added`);
  }

  function deleteBudget(id: string) {
    const updated = budgets.filter((b) => b.id !== id);
    setBudgets(updated);
    saveBudgets(updated);
    toast.success("Budget removed");
  }

  function getActualForBudget(budget: Budget): number {
    const base = actualSpend[budget.category] || 0;
    if (budget.period === "monthly") return base;
    if (budget.period === "quarterly") return base; // Show monthly actuals against quarterly budget
    if (budget.period === "yearly") return base;
    return base;
  }

  function getBudgetPeriodAmount(budget: Budget): number {
    // Normalize budget to monthly for comparison
    if (budget.period === "quarterly") return budget.amount / 3;
    if (budget.period === "yearly") return budget.amount / 12;
    return budget.amount;
  }

  const overBudgetCount = budgets.filter((b) => {
    const actual = getActualForBudget(b);
    const monthly = getBudgetPeriodAmount(b);
    return actual > monthly;
  }).length;

  const onTrackCount = budgets.length - overBudgetCount;

  const selectClass =
    "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#E8453C]/30 focus:border-[#E8453C]";

  return (
    <div className="space-y-6">
      <PageHeader
        title="Budget Tracker"
        description="Set budgets for cloud spend categories and track actual costs against targets."
        icon={Target}
        breadcrumbs={["FinOps", "Budgets & Alerts", "Budgets"]}
        actions={
          <Button
            size="sm"
            onClick={() => setShowForm(!showForm)}
            className="bg-[#E8453C] hover:bg-[#D93025] text-white"
          >
            <Plus className="h-3.5 w-3.5 mr-1.5" /> Add Budget
          </Button>
        }
      />

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading cost data...
        </div>
      ) : (
        <>
          {/* Summary KPIs */}
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <SummaryCard label="Total Budgets" value={budgets.length} icon={PiggyBank} />
            <SummaryCard label="Over Budget" value={overBudgetCount} color={overBudgetCount > 0 ? "red" : "green"} icon={AlertTriangle} />
            <SummaryCard label="On Track" value={onTrackCount} color="green" icon={CheckCircle} />
          </div>

          {/* Add Budget Form */}
          {showForm && (
            <Card className="border-[#E8453C]/30">
              <CardHeader className="pb-2">
                <CardTitle className="text-base">New Budget</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
                  <div>
                    <label className="text-sm font-medium mb-1 block">Name</label>
                    <Input
                      value={formName}
                      onChange={(e) => setFormName(e.target.value)}
                      placeholder="e.g. Monthly Cloud Budget"
                      className="h-9"
                    />
                  </div>
                  <div>
                    <label className="text-sm font-medium mb-1 block">Amount ($)</label>
                    <Input
                      type="number"
                      value={formAmount}
                      onChange={(e) => setFormAmount(e.target.value)}
                      placeholder="1000"
                      min={0}
                      className="h-9"
                    />
                  </div>
                  <div>
                    <label className="text-sm font-medium mb-1 block">Period</label>
                    <select
                      className={selectClass}
                      value={formPeriod}
                      onChange={(e) => setFormPeriod(e.target.value as any)}
                    >
                      <option value="monthly">Monthly</option>
                      <option value="quarterly">Quarterly</option>
                      <option value="yearly">Yearly</option>
                    </select>
                  </div>
                  <div>
                    <label className="text-sm font-medium mb-1 block">Category</label>
                    <select
                      className={selectClass}
                      value={formCategory}
                      onChange={(e) => setFormCategory(e.target.value as any)}
                    >
                      <option value="total">Total</option>
                      <option value="databricks">Databricks</option>
                      <option value="storage">Storage</option>
                      <option value="compute">Compute</option>
                    </select>
                  </div>
                  <div className="flex items-end">
                    <Button
                      onClick={addBudget}
                      className="w-full bg-[#E8453C] hover:bg-[#D93025] text-white h-9"
                    >
                      <Plus className="h-3.5 w-3.5 mr-1" /> Add
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Budget Cards */}
          {budgets.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
              <Target className="h-10 w-10 mb-3 opacity-40" />
              <p className="text-sm">No budgets configured yet.</p>
              <p className="text-xs mt-1">Click "Add Budget" to create your first budget.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {budgets.map((budget) => {
                const actual = getActualForBudget(budget);
                const monthlyBudget = getBudgetPeriodAmount(budget);
                const pct = monthlyBudget > 0 ? (actual / monthlyBudget) * 100 : 0;
                const barWidth = Math.min(pct, 100);

                return (
                  <Card key={budget.id} className={`${pct >= 100 ? "border-red-500/30" : pct >= 80 ? "border-amber-500/30" : ""}`}>
                    <CardContent className="pt-5 pb-4">
                      <div className="flex items-start justify-between mb-3">
                        <div>
                          <p className="text-sm font-medium">{budget.name}</p>
                          <div className="flex items-center gap-2 mt-0.5">
                            <Badge variant="outline" className="text-[10px]">{budget.period}</Badge>
                            <Badge variant="outline" className="text-[10px] capitalize">{budget.category}</Badge>
                          </div>
                        </div>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => deleteBudget(budget.id)}
                          className="h-7 w-7 p-0 text-muted-foreground hover:text-red-500"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>

                      {/* Progress bar */}
                      <div className="w-full h-3 rounded-full bg-muted/50 overflow-hidden mb-2">
                        <div
                          className={`h-full rounded-full transition-all ${progressColor(pct)}`}
                          style={{ width: `${barWidth}%` }}
                        />
                      </div>

                      <div className="flex items-center justify-between text-xs">
                        <span className={progressTextColor(pct)}>
                          <DollarSign className="h-3 w-3 inline" />
                          {actual.toFixed(2)} actual
                        </span>
                        <span className="text-muted-foreground">
                          <DollarSign className="h-3 w-3 inline" />
                          {monthlyBudget.toFixed(2)} budget{budget.period !== "monthly" ? "/mo" : ""}
                        </span>
                      </div>

                      <div className="mt-1 text-right">
                        <span className={`text-xs font-semibold ${progressTextColor(pct)}`}>
                          {pct.toFixed(0)}% utilized
                        </span>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
        </>
      )}
    </div>
  );
}
