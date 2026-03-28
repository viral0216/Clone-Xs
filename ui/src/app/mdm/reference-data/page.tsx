// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { BookOpen, Plus, Trash2, Search, Edit3, Check } from "lucide-react";

const INITIAL_CODE_LISTS = [
  { id: "CL-001", name: "Country Codes", description: "ISO 3166-1 alpha-2", entries: 249, domain: "Global",
    items: [{ code: "US", value: "United States", aliases: ["USA", "U.S.", "America"] }, { code: "GB", value: "United Kingdom", aliases: ["UK", "Britain"] }, { code: "DE", value: "Germany", aliases: ["Deutschland"] }, { code: "FR", value: "France", aliases: [] }] },
  { id: "CL-002", name: "Industry Codes", description: "NAICS sectors", entries: 20, domain: "Customer",
    items: [{ code: "51", value: "Information", aliases: ["IT", "Tech", "Technology"] }, { code: "52", value: "Finance and Insurance", aliases: ["Financial Services", "Banking"] }, { code: "62", value: "Health Care", aliases: ["Healthcare", "Medical"] }] },
  { id: "CL-003", name: "Currency Codes", description: "ISO 4217", entries: 180, domain: "Global",
    items: [{ code: "USD", value: "US Dollar", aliases: ["$", "Dollar"] }, { code: "EUR", value: "Euro", aliases: ["€"] }, { code: "GBP", value: "British Pound", aliases: ["£", "Pound Sterling"] }] },
  { id: "CL-004", name: "Status Codes", description: "Entity lifecycle states", entries: 5, domain: "MDM",
    items: [{ code: "ACT", value: "Active", aliases: ["active", "enabled"] }, { code: "INA", value: "Inactive", aliases: ["inactive", "disabled"] }, { code: "DEL", value: "Deleted", aliases: ["removed"] }] },
];

const INITIAL_MAPPINGS = [
  { id: "MAP-001", name: "CRM Status → MDM Status", source: "CRM", target: "MDM", mappings: [{ from: "Open", to: "ACT" }, { from: "Closed", to: "INA" }, { from: "Churned", to: "DEL" }] },
  { id: "MAP-002", name: "ERP Country → ISO Country", source: "ERP", target: "Global", mappings: [{ from: "United States of America", to: "US" }, { from: "Great Britain", to: "GB" }] },
];

export default function ReferenceDataPage() {
  const [codeLists] = useState(INITIAL_CODE_LISTS);
  const [mappings] = useState(INITIAL_MAPPINGS);
  const [selectedList, setSelectedList] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [tab, setTab] = useState<"lists" | "mappings">("lists");

  const selected = codeLists.find(c => c.id === selectedList);

  return (
    <div className="space-y-4">
      <PageHeader title="Reference Data Management" icon={BookOpen} breadcrumbs={["MDM", "Reference Data"]}
        description="Manage code lists, cross-reference tables, and standardization mappings between source systems." />

      <div className="flex gap-1 border-b border-border">
        {[{ key: "lists", label: `Code Lists (${codeLists.length})` }, { key: "mappings", label: `Mappings (${mappings.length})` }].map(t => (
          <button key={t.key} onClick={() => setTab(t.key as any)}
            className={`px-3 py-2 text-xs font-medium border-b-2 transition-colors ${tab === t.key ? "border-[#E8453C] text-[#E8453C]" : "border-transparent text-muted-foreground hover:text-foreground"}`}>{t.label}</button>
        ))}
      </div>

      {tab === "lists" && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="space-y-2">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <input className="w-full pl-8 pr-3 py-1.5 text-sm bg-muted border border-border rounded-md" placeholder="Search code lists..." value={search} onChange={e => setSearch(e.target.value)} />
            </div>
            {codeLists.filter(c => c.name.toLowerCase().includes(search.toLowerCase())).map(cl => (
              <Card key={cl.id} className={`cursor-pointer transition-colors ${selectedList === cl.id ? "border-[#E8453C]/50" : "hover:border-border"}`} onClick={() => setSelectedList(cl.id)}>
                <CardContent className="pt-3 pb-3">
                  <p className="text-sm font-medium">{cl.name}</p>
                  <p className="text-xs text-muted-foreground">{cl.description}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <Badge variant="outline" className="text-[10px]">{cl.domain}</Badge>
                    <span className="text-[10px] text-muted-foreground">{cl.entries} entries</span>
                  </div>
                </CardContent>
              </Card>
            ))}
            <Button size="sm" variant="outline" className="w-full h-8 text-xs"><Plus className="h-3 w-3 mr-1" /> New Code List</Button>
          </div>
          <div className="md:col-span-2">
            {selected ? (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm">{selected.name}</CardTitle>
                    <Button size="sm" variant="outline" className="h-7 text-xs"><Plus className="h-3 w-3 mr-1" /> Add Entry</Button>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md border border-border overflow-hidden">
                    <div className="grid grid-cols-3 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                      <span>Code</span><span>Value</span><span>Aliases</span>
                    </div>
                    {selected.items.map((item, i) => (
                      <div key={i} className="grid grid-cols-3 items-center px-3 py-2 border-t border-border hover:bg-muted/20">
                        <span className="text-xs font-mono font-medium">{item.code}</span>
                        <span className="text-xs">{item.value}</span>
                        <div className="flex items-center gap-1 flex-wrap">
                          {item.aliases.map((a, j) => <Badge key={j} variant="outline" className="text-[9px]">{a}</Badge>)}
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            ) : (
              <Card><CardContent className="py-12 text-center text-sm text-muted-foreground">Select a code list to view entries</CardContent></Card>
            )}
          </div>
        </div>
      )}

      {tab === "mappings" && (
        <div className="space-y-3">
          {mappings.map(m => (
            <Card key={m.id}>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  {m.name}
                  <Badge variant="outline" className="text-[10px]">{m.source} → {m.target}</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="rounded-md border border-border overflow-hidden">
                  <div className="grid grid-cols-2 bg-muted/30 px-3 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase">
                    <span>Source Value ({m.source})</span><span>Target Code ({m.target})</span>
                  </div>
                  {m.mappings.map((mp, i) => (
                    <div key={i} className="grid grid-cols-2 px-3 py-2 border-t border-border">
                      <span className="text-xs">{mp.from}</span>
                      <span className="text-xs font-mono">{mp.to}</span>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
          <Button size="sm" variant="outline" className="h-8 text-xs"><Plus className="h-3 w-3 mr-1" /> New Mapping</Button>
        </div>
      )}
    </div>
  );
}
