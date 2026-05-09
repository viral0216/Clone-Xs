// @ts-nocheck
//
// Catalog / Schema / Volume picker — reusable across the five
// unstructured-demo tabs (Documents / Media / Knowledge / Logs / Code).
//
// Each field is a dropdown of existing names with a "Custom name…"
// fallback that swaps in a free-text input for the typed-in name.
// Mirrors the pattern the Streaming Events tab uses (see
// page.tsx :2575+) but extracted so the unstructured tabs stay
// terse.
//
// Backend assumptions:
//   - GET /api/catalogs                         → string[]
//   - GET /api/catalogs/{catalog}/schemas       → string[]
//   - GET /api/auth/volumes                     → VolumeInfo[]
//
// All three already exist in the API surface.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { useVolumes } from "@/hooks/useApi";

interface Props {
  catalog: string;
  setCatalog: (v: string) => void;
  schema: string;
  setSchema: (v: string) => void;
  volume: string;
  setVolume: (v: string) => void;
  /**
   * When false, the Volume picker is disabled (used by direct_table
   * destination — Volume isn't needed). The label still renders so
   * the layout stays stable.
   */
  volumeEnabled?: boolean;
  /** Default name shown for the Volume "default" option. */
  defaultVolumeName?: string;
}

export default function CatalogSchemaVolumePicker({
  catalog,
  setCatalog,
  schema,
  setSchema,
  volume,
  setVolume,
  volumeEnabled = true,
  defaultVolumeName = "demo_unstructured",
}: Props) {
  const [catalogCustom, setCatalogCustom] = useState(false);
  const [schemaCustom, setSchemaCustom] = useState(false);
  const [volumeCustom, setVolumeCustom] = useState(false);

  const volumesQuery = useVolumes();
  const catalogsQuery = useQuery<string[]>({
    queryKey: ["catalogs"],
    queryFn: () => api.get("/catalogs"),
    staleTime: 1000 * 60 * 10,
  });
  const schemasQuery = useQuery<string[]>({
    queryKey: ["schemas", catalog],
    queryFn: () => api.get(`/catalogs/${catalog}/schemas`),
    // Skip the fetch when the user is in "Custom catalog" mode — the
    // catalog doesn't exist yet so there are no schemas to enumerate.
    enabled: !!catalog && !catalogCustom,
    staleTime: 1000 * 60 * 5,
  });

  // Volumes within the chosen catalog.schema (drives the Volume
  // dropdown choices).
  const volumeMatches = (volumesQuery.data || [])
    .filter(
      (v) => (!catalog || v.catalog === catalog) && (!schema || v.schema === schema),
    )
    .map((v) => v.name);
  const uniqueExistingVolumes = Array.from(new Set(volumeMatches));

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
      {/* Catalog */}
      <div>
        <label className="block text-xs font-medium mb-1">Catalog</label>
        <select
          className="w-full h-8 px-2 text-sm bg-background border border-input rounded-md font-mono"
          value={catalogCustom ? "__custom__" : catalog}
          onChange={(e) => {
            const v = e.target.value;
            if (v === "__custom__") {
              setCatalogCustom(true);
            } else {
              setCatalogCustom(false);
              setCatalog(v);
              // Reset schema custom flag when catalog changes — we
              // can't know if the existing schema name applies under
              // the newly-selected catalog.
              setSchemaCustom(false);
              setSchema("");
            }
          }}
        >
          <option value="">
            {catalogsQuery.isLoading ? "Loading…" : "Select catalog…"}
          </option>
          {(catalogsQuery.data || []).map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
          <option value="__custom__">Custom name… (create new)</option>
        </select>
        {catalogCustom && (
          <Input
            value={catalog}
            onChange={(e) => setCatalog(e.target.value)}
            placeholder="my_catalog"
            className="mt-1.5 font-mono text-sm h-8"
            autoFocus
          />
        )}
      </div>

      {/* Schema */}
      <div>
        <label className="block text-xs font-medium mb-1">Schema</label>
        <select
          className="w-full h-8 px-2 text-sm bg-background border border-input rounded-md font-mono"
          value={schemaCustom ? "__custom__" : schema}
          onChange={(e) => {
            const v = e.target.value;
            if (v === "__custom__") {
              setSchemaCustom(true);
            } else {
              setSchemaCustom(false);
              setSchema(v);
            }
          }}
          disabled={!catalog && !catalogCustom}
        >
          <option value="">
            {!catalog && !catalogCustom
              ? "Select catalog first"
              : catalogCustom
                ? "—"
                : schemasQuery.isLoading
                  ? "Loading…"
                  : "Select schema…"}
          </option>
          {!catalogCustom &&
            (schemasQuery.data || []).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          <option value="__custom__">Custom name… (create new)</option>
        </select>
        {schemaCustom && (
          <Input
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            placeholder="my_schema"
            className="mt-1.5 font-mono text-sm h-8"
            autoFocus
          />
        )}
      </div>

      {/* Volume */}
      <div>
        <label className="block text-xs font-medium mb-1">
          Volume {volumeEnabled ? "" : "(unused for direct_table)"}
        </label>
        <select
          className="w-full h-8 px-2 text-sm bg-background border border-input rounded-md font-mono"
          value={volumeCustom ? "__custom__" : volume}
          onChange={(e) => {
            const v = e.target.value;
            if (v === "__custom__") {
              setVolumeCustom(true);
            } else {
              setVolumeCustom(false);
              setVolume(v);
            }
          }}
          disabled={!volumeEnabled}
        >
          <option value={defaultVolumeName}>
            {defaultVolumeName} (default — created if missing)
          </option>
          {uniqueExistingVolumes
            .filter((n) => n !== defaultVolumeName)
            .map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          <option value="__custom__">Custom name…</option>
        </select>
        {volumeCustom && volumeEnabled && (
          <Input
            value={volume}
            onChange={(e) => setVolume(e.target.value)}
            placeholder="my_volume"
            className="mt-1.5 font-mono text-sm h-8"
            autoFocus
          />
        )}
        {volumeEnabled && (
          <p className="text-[10px] text-muted-foreground mt-1">
            {volumesQuery.isLoading
              ? "Loading volumes…"
              : catalog && schema && uniqueExistingVolumes.length === 0
                ? `No existing volumes in ${catalog}.${schema} — default will be created on submit.`
                : `${uniqueExistingVolumes.length} existing volume${
                    uniqueExistingVolumes.length === 1 ? "" : "s"
                  } in scope.`}
          </p>
        )}
      </div>
    </div>
  );
}
