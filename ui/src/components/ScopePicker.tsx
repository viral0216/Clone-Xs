// @ts-nocheck
import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import { useSchemaObjects } from "@/hooks/useApi";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ChevronDown, ChevronRight, Loader2, FolderTree, Table2,
  Eye, FunctionSquare, HardDrive, Info,
} from "lucide-react";

export type ObjectRef = {
  schema: string;
  name: string;
  type: "table" | "view" | "function" | "volume";
};

export type ScopeMode = "all" | "select";

interface Props {
  catalog: string;
  mode: ScopeMode;
  onModeChange: (m: ScopeMode) => void;
  selected: ObjectRef[];
  onSelectedChange: (s: ObjectRef[]) => void;
}

const OBJECT_ICONS: Record<ObjectRef["type"], any> = {
  table: Table2,
  view: Eye,
  function: FunctionSquare,
  volume: HardDrive,
};

export default function ScopePicker({
  catalog,
  mode,
  onModeChange,
  selected,
  onSelectedChange,
}: Props) {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (mode !== "select" || !catalog) return;
    setLoadingSchemas(true);
    api
      .get<string[]>(`/catalogs/${encodeURIComponent(catalog)}/schemas`)
      .then((data) => setSchemas(data || []))
      .catch(() => setSchemas([]))
      .finally(() => setLoadingSchemas(false));
  }, [catalog, mode]);

  const selectedBySchema = new Map<string, Set<string>>();
  for (const o of selected) {
    const key = `${o.schema}\u0000${o.type}`;
    if (!selectedBySchema.has(key)) selectedBySchema.set(key, new Set());
    selectedBySchema.get(key)!.add(o.name);
  }
  const isSelected = (schema: string, name: string, type: ObjectRef["type"]) =>
    selectedBySchema.get(`${schema}\u0000${type}`)?.has(name) ?? false;

  const toggleObject = (schema: string, name: string, type: ObjectRef["type"]) => {
    const match = (o: ObjectRef) =>
      o.schema === schema && o.name === name && o.type === type;
    if (selected.some(match)) {
      onSelectedChange(selected.filter((o) => !match(o)));
    } else {
      onSelectedChange([...selected, { schema, name, type }]);
    }
  };

  const toggleExpand = (schema: string) => {
    const next = new Set(expanded);
    if (next.has(schema)) next.delete(schema);
    else next.add(schema);
    setExpanded(next);
  };

  const schemasInSelection = new Set(selected.map((o) => o.schema));
  const totals = {
    tables: selected.filter((o) => o.type === "table").length,
    views: selected.filter((o) => o.type === "view").length,
    functions: selected.filter((o) => o.type === "function").length,
    volumes: selected.filter((o) => o.type === "volume").length,
  };

  return (
    <div className="space-y-3">
      <div>
        <label className="text-sm font-medium">Scope</label>
        <div className="flex gap-2 mt-1">
          <Button
            type="button"
            size="sm"
            variant={mode === "all" ? "default" : "outline"}
            onClick={() => {
              onModeChange("all");
              onSelectedChange([]);
            }}
          >
            <FolderTree className="h-4 w-4 mr-1.5" /> Entire catalog
          </Button>
          <Button
            type="button"
            size="sm"
            variant={mode === "select" ? "default" : "outline"}
            onClick={() => onModeChange("select")}
          >
            Select schemas + objects
          </Button>
        </div>
      </div>

      {mode === "select" && (
        <div className="space-y-2">
          {!catalog && (
            <div className="text-xs text-muted-foreground italic">
              Select a source catalog first.
            </div>
          )}
          {catalog && loadingSchemas && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading schemas…
            </div>
          )}
          {catalog && !loadingSchemas && schemas.length === 0 && (
            <div className="text-xs text-muted-foreground">
              No schemas visible in <code>{catalog}</code>.
            </div>
          )}
          {catalog && !loadingSchemas && schemas.length > 0 && (
            <>
              <div className="flex items-center gap-3 text-xs text-muted-foreground">
                <span>
                  <Badge variant="outline">
                    {schemasInSelection.size} / {schemas.length} schemas
                  </Badge>
                </span>
                <span>{totals.tables} tables</span>
                <span>{totals.views} views</span>
                <span>{totals.functions} functions</span>
                <span>{totals.volumes} volumes</span>
              </div>
              <div className="border rounded-md max-h-80 overflow-auto divide-y">
                {schemas.map((schema) => (
                  <SchemaRow
                    key={schema}
                    catalog={catalog}
                    schema={schema}
                    expanded={expanded.has(schema)}
                    onToggleExpand={() => toggleExpand(schema)}
                    isSelected={(name, type) => isSelected(schema, name, type)}
                    onToggleObject={(name, type) => toggleObject(schema, name, type)}
                    selected={selected}
                    onSelectedChange={onSelectedChange}
                  />
                ))}
              </div>
              <div className="flex items-start gap-2 text-xs text-muted-foreground bg-muted/40 rounded-md p-2">
                <Info className="h-4 w-4 mt-0.5 shrink-0" />
                <span>
                  Selected objects are passed as <code>include_objects</code>. Backend translates them into <code>include_schemas</code> + a table-name regex. Volumes are enumerated per-schema and don't honor the regex today — selecting a specific volume includes the whole schema's volumes.
                </span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

interface SchemaRowProps {
  catalog: string;
  schema: string;
  expanded: boolean;
  onToggleExpand: () => void;
  isSelected: (name: string, type: ObjectRef["type"]) => boolean;
  onToggleObject: (name: string, type: ObjectRef["type"]) => void;
  selected: ObjectRef[];
  onSelectedChange: (s: ObjectRef[]) => void;
}

function SchemaRow({
  catalog,
  schema,
  expanded,
  onToggleExpand,
  isSelected,
  onToggleObject,
  selected,
  onSelectedChange,
}: SchemaRowProps) {
  const { data, isLoading } = useSchemaObjects(
    expanded ? catalog : null,
    expanded ? schema : null,
  );

  const schemaObjectCount = selected.filter((o) => o.schema === schema).length;

  const selectAll = () => {
    if (!data) return;
    const all: ObjectRef[] = [
      ...data.tables.map((n) => ({ schema, name: n, type: "table" as const })),
      ...data.views.map((n) => ({ schema, name: n, type: "view" as const })),
      ...data.functions.map((n) => ({ schema, name: n, type: "function" as const })),
      ...data.volumes.map((n) => ({ schema, name: n, type: "volume" as const })),
    ];
    const others = selected.filter((o) => o.schema !== schema);
    onSelectedChange([...others, ...all]);
  };

  const clearSchema = () => {
    onSelectedChange(selected.filter((o) => o.schema !== schema));
  };

  return (
    <div className="text-sm">
      <div className="flex items-center gap-2 px-3 py-2 hover:bg-muted/30 cursor-pointer">
        <button
          type="button"
          onClick={onToggleExpand}
          className="flex items-center gap-1"
        >
          {expanded ? (
            <ChevronDown className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
          <FolderTree className="h-4 w-4 text-muted-foreground" />
          <span className="font-medium">{schema}</span>
        </button>
        {schemaObjectCount > 0 && (
          <Badge variant="secondary" className="ml-auto text-xs">
            {schemaObjectCount} selected
          </Badge>
        )}
        {expanded && data && (
          <div className="flex items-center gap-1 ml-2">
            <button
              type="button"
              onClick={selectAll}
              className="text-xs text-[#E8453C] hover:underline"
            >
              all
            </button>
            <span className="text-muted-foreground">·</span>
            <button
              type="button"
              onClick={clearSchema}
              className="text-xs text-muted-foreground hover:underline"
            >
              none
            </button>
          </div>
        )}
      </div>

      {expanded && (
        <div className="pl-10 pr-3 pb-2 space-y-0.5">
          {isLoading && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground py-1">
              <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading…
            </div>
          )}
          {data && (
            <>
              <ObjectGroup
                kind="table" items={data.tables}
                isSelected={isSelected} onToggle={onToggleObject}
              />
              <ObjectGroup
                kind="view" items={data.views}
                isSelected={isSelected} onToggle={onToggleObject}
              />
              <ObjectGroup
                kind="function" items={data.functions}
                isSelected={isSelected} onToggle={onToggleObject}
              />
              <ObjectGroup
                kind="volume" items={data.volumes}
                isSelected={isSelected} onToggle={onToggleObject}
              />
              {data.tables.length === 0 &&
                data.views.length === 0 &&
                data.functions.length === 0 &&
                data.volumes.length === 0 && (
                  <div className="text-xs text-muted-foreground italic py-1">
                    No objects in this schema.
                  </div>
                )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

interface ObjectGroupProps {
  kind: ObjectRef["type"];
  items: string[];
  isSelected: (name: string, type: ObjectRef["type"]) => boolean;
  onToggle: (name: string, type: ObjectRef["type"]) => void;
}

function ObjectGroup({ kind, items, isSelected, onToggle }: ObjectGroupProps) {
  if (items.length === 0) return null;
  const Icon = OBJECT_ICONS[kind];
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mt-1">
        {kind}s ({items.length})
      </div>
      {items.map((name) => (
        <label
          key={name}
          className="flex items-center gap-2 text-xs py-0.5 cursor-pointer hover:text-foreground"
        >
          <input
            type="checkbox"
            checked={isSelected(name, kind)}
            onChange={() => onToggle(name, kind)}
          />
          <Icon className="h-3.5 w-3.5 text-muted-foreground" />
          <span>{name}</span>
        </label>
      ))}
    </div>
  );
}
