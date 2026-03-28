// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { LayoutTemplate, Heart, Building2, ShoppingCart, Factory, Stethoscope, Check } from "lucide-react";
import { useCreateRule } from "@/hooks/useMdm";

const TEMPLATES = [
  { id: "healthcare", name: "Healthcare — Patient MPI", icon: Stethoscope, color: "text-red-500",
    description: "Master Patient Index for healthcare. Match on name, DOB, SSN, MRN, address. HIPAA-aware masking.",
    entityType: "Patient", domains: ["Patient", "Provider", "Facility"],
    rules: [
      { name: "exact_mrn", field: "mrn", match_type: "exact", weight: 1.0 },
      { name: "exact_ssn", field: "ssn", match_type: "exact", weight: 1.0 },
      { name: "fuzzy_name", field: "name", match_type: "fuzzy_jaro_winkler", weight: 0.85 },
      { name: "exact_dob", field: "date_of_birth", match_type: "exact", weight: 0.9 },
      { name: "fuzzy_address", field: "address", match_type: "normalized", weight: 0.7 },
    ],
    survivorship: [{ field: "name", strategy: "most_complete" }, { field: "address", strategy: "most_recent" }, { field: "phone", strategy: "most_trusted" }] },
  { id: "financial", name: "Financial Services — KYC/AML", icon: Building2, color: "text-blue-500",
    description: "Know Your Customer entity resolution. Match on legal name, LEI, tax ID, address. AML screening integration.",
    entityType: "Customer", domains: ["Customer", "Legal Entity", "Beneficial Owner"],
    rules: [
      { name: "exact_tax_id", field: "tax_id", match_type: "exact", weight: 1.0 },
      { name: "exact_lei", field: "lei_code", match_type: "exact", weight: 1.0 },
      { name: "normalized_name", field: "legal_name", match_type: "normalized", weight: 0.9 },
      { name: "fuzzy_name", field: "name", match_type: "fuzzy_levenshtein", weight: 0.8 },
      { name: "exact_country", field: "country", match_type: "exact", weight: 0.5 },
    ],
    survivorship: [{ field: "legal_name", strategy: "most_trusted" }, { field: "address", strategy: "most_recent" }, { field: "revenue", strategy: "most_trusted" }] },
  { id: "retail", name: "Retail — Customer 360", icon: ShoppingCart, color: "text-green-500",
    description: "Unified customer view across POS, e-commerce, loyalty, and CRM. Household grouping support.",
    entityType: "Customer", domains: ["Customer", "Household", "Product"],
    rules: [
      { name: "exact_email", field: "email", match_type: "exact", weight: 1.0 },
      { name: "exact_phone", field: "phone", match_type: "numeric", weight: 0.95 },
      { name: "fuzzy_name", field: "name", match_type: "fuzzy_jaro_winkler", weight: 0.85 },
      { name: "exact_loyalty_id", field: "loyalty_id", match_type: "exact", weight: 1.0 },
      { name: "normalized_address", field: "address", match_type: "normalized", weight: 0.7 },
    ],
    survivorship: [{ field: "email", strategy: "most_recent" }, { field: "name", strategy: "longest" }, { field: "address", strategy: "most_complete" }] },
  { id: "manufacturing", name: "Manufacturing — Supplier MDM", icon: Factory, color: "text-orange-500",
    description: "Supplier and part master. Match on DUNS number, company name, address. BOM hierarchy support.",
    entityType: "Supplier", domains: ["Supplier", "Part", "BOM"],
    rules: [
      { name: "exact_duns", field: "duns_number", match_type: "exact", weight: 1.0 },
      { name: "normalized_company", field: "company_name", match_type: "normalized", weight: 0.9 },
      { name: "fuzzy_company", field: "company_name", match_type: "fuzzy_levenshtein", weight: 0.8 },
      { name: "exact_country", field: "country", match_type: "exact", weight: 0.5 },
      { name: "exact_part_number", field: "part_number", match_type: "exact", weight: 1.0 },
    ],
    survivorship: [{ field: "company_name", strategy: "most_trusted" }, { field: "address", strategy: "most_complete" }, { field: "contact", strategy: "most_recent" }] },
];

export default function TemplatesPage() {
  const createRule = useCreateRule();
  const [applied, setApplied] = useState<Set<string>>(new Set());

  const applyTemplate = async (template: typeof TEMPLATES[0]) => {
    for (const rule of template.rules) {
      await createRule.mutateAsync({ entity_type: template.entityType, name: rule.name, field: rule.field, match_type: rule.match_type, weight: rule.weight });
    }
    setApplied(prev => new Set([...prev, template.id]));
  };

  return (
    <div className="space-y-4">
      <PageHeader title="Industry Templates" icon={LayoutTemplate} breadcrumbs={["MDM", "Templates"]}
        description="Pre-built entity models with matching rules and survivorship strategies for common industries." />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {TEMPLATES.map(t => {
          const Icon = t.icon;
          const isApplied = applied.has(t.id);
          return (
            <Card key={t.id} className={isApplied ? "border-[#E8453C]/30" : ""}>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  <Icon className={`h-4 w-4 ${t.color}`} />
                  {t.name}
                  {isApplied && <Badge className="bg-muted/40 text-foreground border-border text-[10px]"><Check className="h-3 w-3 mr-0.5" /> Applied</Badge>}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <p className="text-xs text-muted-foreground">{t.description}</p>
                <div className="flex gap-1 flex-wrap">
                  {t.domains.map(d => <Badge key={d} variant="outline" className="text-[10px]">{d}</Badge>)}
                </div>
                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase mb-1">Matching Rules ({t.rules.length})</p>
                  <div className="space-y-0.5">
                    {t.rules.map(r => (
                      <div key={r.name} className="flex items-center gap-2 text-xs">
                        <span className="font-mono text-muted-foreground">{r.name}</span>
                        <span className="text-muted-foreground">→</span>
                        <span>{r.match_type}</span>
                        <span className="text-muted-foreground">on</span>
                        <span className="font-medium">{r.field}</span>
                        <span className="text-muted-foreground ml-auto">w={r.weight}</span>
                      </div>
                    ))}
                  </div>
                </div>
                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase mb-1">Survivorship</p>
                  <div className="flex gap-1.5 flex-wrap">
                    {t.survivorship.map(s => <Badge key={s.field} variant="outline" className="text-[9px]">{s.field}: {s.strategy}</Badge>)}
                  </div>
                </div>
                <Button size="sm" className="w-full" disabled={isApplied || createRule.isPending} onClick={() => applyTemplate(t)}>
                  {isApplied ? "Applied" : "Apply Template"}
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
