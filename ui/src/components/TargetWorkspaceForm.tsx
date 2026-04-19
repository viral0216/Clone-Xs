// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useValidateTarget } from "@/hooks/useApi";
import { toast } from "sonner";
import { Globe2, CheckCircle2, XCircle, Loader2, Info } from "lucide-react";

export type TargetWorkspaceValue = {
  host: string;
  auth_method: "pat" | "service_principal" | "profile";
  token?: string;
  client_id?: string;
  client_secret?: string;
  profile?: string;
  warehouse_id: string;
  keep_share?: boolean;
};

interface Props {
  enabled: boolean;
  onEnabledChange: (v: boolean) => void;
  value: TargetWorkspaceValue;
  onChange: (v: TargetWorkspaceValue) => void;
  validated: boolean;
  onValidatedChange: (v: boolean) => void;
}

const AUTH_METHODS = [
  { value: "pat", label: "Personal Access Token" },
  { value: "service_principal", label: "Service Principal" },
  { value: "profile", label: "CLI Profile" },
] as const;

export default function TargetWorkspaceForm({
  enabled,
  onEnabledChange,
  value,
  onChange,
  validated,
  onValidatedChange,
}: Props) {
  const validate = useValidateTarget();
  const [lastCheckedKey, setLastCheckedKey] = useState<string>("");

  const update = (patch: Partial<TargetWorkspaceValue>) => {
    onChange({ ...value, ...patch });
    // Any change invalidates a previous successful test.
    if (validated) onValidatedChange(false);
  };

  const currentKey = JSON.stringify({
    host: value.host,
    auth: value.auth_method,
    token: value.token,
    ci: value.client_id,
    cs: value.client_secret,
    pf: value.profile,
  });

  const runValidate = () => {
    const payload: Record<string, unknown> = {
      host: value.host,
      auth_method: value.auth_method,
      warehouse_id: value.warehouse_id,
    };
    if (value.auth_method === "pat") payload.token = value.token;
    if (value.auth_method === "service_principal") {
      payload.client_id = value.client_id;
      payload.client_secret = value.client_secret;
    }
    if (value.auth_method === "profile") payload.profile = value.profile;

    validate.mutate(payload, {
      onSuccess: (data: any) => {
        onValidatedChange(true);
        setLastCheckedKey(currentKey);
        const extras: string[] = [];
        if (typeof data?.catalog_count === "number") extras.push(`${data.catalog_count} catalogs visible`);
        if (data?.metastore_sharing_id) extras.push("sharing enabled");
        toast.success(`Target connection OK${extras.length ? ` — ${extras.join(", ")}` : ""}`);
      },
      onError: (e: any) => {
        onValidatedChange(false);
        toast.error(e?.message || "Target validation failed");
      },
    });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Globe2 className="h-5 w-5" />
          Target Workspace (optional)
          {enabled && validated && (
            <Badge className="bg-green-100 text-green-700 dark:bg-green-950/50 dark:text-green-400 border-green-200">
              Connected
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <label className="flex items-start gap-2 cursor-pointer">
          <input
            type="checkbox"
            className="mt-1"
            checked={enabled}
            onChange={(e) => {
              onEnabledChange(e.target.checked);
              if (!e.target.checked) onValidatedChange(false);
            }}
          />
          <div>
            <div className="text-sm font-medium">Clone to a different workspace</div>
            <div className="text-xs text-muted-foreground">
              Cross-workspace / cross-cloud migration via Delta Sharing → DEEP CLONE. Data physically lands in the target cloud's storage.
            </div>
          </div>
        </label>

        {enabled && (
          <div className="space-y-3 border-t pt-4">
            <div>
              <label className="text-sm font-medium">Target Host</label>
              <Input
                placeholder="https://adb-xxxx.azuredatabricks.net"
                value={value.host}
                onChange={(e) => update({ host: e.target.value })}
              />
              <p className="text-xs text-gray-400 mt-1">
                Full workspace URL of the destination (different workspace, can be a different cloud).
              </p>
            </div>

            <div>
              <label className="text-sm font-medium">Auth Method</label>
              <div className="flex gap-2 mt-1">
                {AUTH_METHODS.map((m) => (
                  <Button
                    key={m.value}
                    size="sm"
                    variant={value.auth_method === m.value ? "default" : "outline"}
                    onClick={() => update({ auth_method: m.value })}
                    type="button"
                  >
                    {m.label}
                  </Button>
                ))}
              </div>
            </div>

            {value.auth_method === "pat" && (
              <div>
                <label className="text-sm font-medium">Personal Access Token</label>
                <Input
                  type="password"
                  placeholder="dapi..."
                  value={value.token || ""}
                  onChange={(e) => update({ token: e.target.value })}
                />
              </div>
            )}

            {value.auth_method === "service_principal" && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <label className="text-sm font-medium">Client ID</label>
                  <Input
                    placeholder="uuid"
                    value={value.client_id || ""}
                    onChange={(e) => update({ client_id: e.target.value })}
                  />
                </div>
                <div>
                  <label className="text-sm font-medium">Client Secret</label>
                  <Input
                    type="password"
                    placeholder="secret"
                    value={value.client_secret || ""}
                    onChange={(e) => update({ client_secret: e.target.value })}
                  />
                </div>
              </div>
            )}

            {value.auth_method === "profile" && (
              <div>
                <label className="text-sm font-medium">CLI Profile</label>
                <Input
                  placeholder="e.g. target-workspace"
                  value={value.profile || ""}
                  onChange={(e) => update({ profile: e.target.value })}
                />
                <p className="text-xs text-gray-400 mt-1">
                  A profile from <code>~/.databrickscfg</code> on the server running Clone-Xs.
                </p>
              </div>
            )}

            <div>
              <label className="text-sm font-medium">Target SQL Warehouse ID</label>
              <Input
                placeholder="e.g. 1234567890abcdef"
                value={value.warehouse_id || ""}
                onChange={(e) => update({ warehouse_id: e.target.value })}
              />
              <p className="text-xs text-gray-400 mt-1">
                DDL (CREATE CATALOG, CREATE SCHEMA) and DEEP CLONE SQL run on this warehouse in the target workspace.
              </p>
            </div>

            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={!!value.keep_share}
                onChange={(e) => update({ keep_share: e.target.checked })}
              />
              Keep migration share after clone (for audit / debugging)
            </label>

            <div className="flex items-center gap-3 pt-2">
              <Button
                type="button"
                variant="outline"
                onClick={runValidate}
                disabled={validate.isPending || !value.host || !value.warehouse_id}
              >
                {validate.isPending ? (
                  <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Testing…</>
                ) : validated ? (
                  <><CheckCircle2 className="h-4 w-4 text-green-600 mr-1" /> Connection OK</>
                ) : (
                  <>Test connection</>
                )}
              </Button>
              {validate.isError && (
                <span className="text-xs text-red-600 inline-flex items-center gap-1">
                  <XCircle className="h-4 w-4" />
                  {(validate.error as any)?.message || "Failed"}
                </span>
              )}
            </div>

            <div className="flex items-start gap-2 text-xs text-muted-foreground bg-muted/40 rounded-md p-3">
              <Info className="h-4 w-4 mt-0.5 shrink-0" />
              <span>
                Migrates schemas + tables (DEEP CLONE via Delta Sharing), views, SQL functions, volumes + files, and replays grants / ownership / tags. Cross-cloud egress applies to file copies.
              </span>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}