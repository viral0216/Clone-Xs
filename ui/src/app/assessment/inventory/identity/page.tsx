// @ts-nocheck
"use client";
import { Users } from "lucide-react";
import WorkspaceCategoryPage from "../WorkspaceCategoryPage";

function ts(ms: number) {
  if (!ms || ms < 0) return "No expiry";
  return new Date(ms).toLocaleDateString();
}

function scimGroups(user: any) {
  const grps = user.groups ?? [];
  return grps.length ? grps.slice(0, 3).map((g: any) => g.display || g.value).join(", ") + (grps.length > 3 ? `…+${grps.length - 3}` : "") : "—";
}

function hasRole(user: any, role: string) {
  return (user.roles ?? []).some((r: any) => r.value === role);
}

export default function IdentityPage() {
  return (
    <WorkspaceCategoryPage
      title="Identity & Access"
      description="Users, groups, service principals, and personal access tokens in this workspace."
      icon={Users}
      breadcrumb="Identity"
      findingCategories={["Identity & Access", "Secrets & Credentials", "Account Governance"]}
      sections={[
        {
          title: "Users",
          resourceType: "users",
          emptyMsg: "No users found.",
          getLink: (_r, ws) => `${ws}/settings/workspace/identity-and-access/users`,
          columns: [
            { key: "userName",    label: "Email / Username", render: r => <span className="font-medium">{r.userName}</span> },
            { key: "displayName", label: "Display Name",     render: r => <span className="text-muted-foreground">{r.displayName ?? "—"}</span> },
            { key: "admin",       label: "Admin",            render: r => hasRole(r, "workspace_admin") ? <span className="text-xs font-medium text-red-600">Admin</span> : <span className="text-muted-foreground/40">—</span> },
            { key: "groups",      label: "Groups",           render: r => <span className="text-muted-foreground">{scimGroups(r)}</span> },
            { key: "externalId",  label: "External ID (SSO)",render: r => r.externalId ? <span className="text-green-600 text-[11px] font-mono">{r.externalId}</span> : <span className="text-muted-foreground/40">None (local)</span> },
          ],
        },
        {
          title: "Groups",
          resourceType: "groups",
          emptyMsg: "No groups configured.",
          getLink: (_r, ws) => `${ws}/settings/workspace/identity-and-access/groups`,
          columns: [
            { key: "displayName", label: "Group Name",   render: r => <span className="font-medium">{r.displayName}</span> },
            { key: "members",     label: "Members",      render: r => {
              const m = r.members ?? [];
              return m.length
                ? <span className="text-muted-foreground">{m.slice(0, 3).map((x: any) => x.display || x.value).join(", ")}{m.length > 3 ? ` +${m.length - 3} more` : ""}</span>
                : <span className="text-yellow-600 text-[11px]">Empty group</span>;
            }},
            { key: "roles",       label: "Entitlements", render: r => {
              const e = r.entitlements ?? [];
              return e.length ? <span className="text-muted-foreground text-[11px]">{e.map((x: any) => x.value).join(", ")}</span> : <span className="text-muted-foreground/40">—</span>;
            }},
          ],
        },
        {
          title: "Service Principals",
          resourceType: "service_principals",
          emptyMsg: "No service principals configured.",
          getLink: (_r, ws) => `${ws}/settings/workspace/identity-and-access/service-principals`,
          columns: [
            { key: "displayName", label: "Name",         render: r => <span className="font-medium">{r.displayName}</span> },
            { key: "applicationId", label: "App ID",     render: r => <span className="font-mono text-[11px] text-muted-foreground">{r.applicationId ?? r.id}</span> },
            { key: "active",      label: "Active",       render: r => r.active !== false ? <span className="text-green-600 text-xs">Active</span> : <span className="text-red-600 text-xs">Inactive</span> },
            { key: "groups",      label: "Groups",       render: r => <span className="text-muted-foreground">{scimGroups(r)}</span> },
            { key: "externalId",  label: "External ID",  render: r => r.externalId ? <span className="font-mono text-[10px] text-muted-foreground">{r.externalId}</span> : <span className="text-muted-foreground/40">—</span> },
          ],
        },
        {
          title: "Personal Access Tokens",
          resourceType: "tokens",
          emptyMsg: "No PAT tokens found.",
          getLink: (_r, ws) => `${ws}/settings/user/personal-access-tokens`,
          columns: [
            { key: "comment",          label: "Comment / Purpose", render: r => <span className="font-medium">{r.comment || "—"}</span> },
            { key: "created_by",       label: "Owner",             render: r => <span className="text-muted-foreground">{r.created_by_username ?? r.owner_id ?? "—"}</span> },
            { key: "expiry_time",      label: "Expires",           render: r => {
              const exp = r.expiry_time;
              if (!exp || exp < 0) return <span className="text-red-600 text-xs font-medium">No expiry ⚠</span>;
              const days = Math.round((exp - Date.now()) / 86400000);
              const color = days < 7 ? "text-red-600" : days < 30 ? "text-yellow-600" : "text-muted-foreground";
              return <span className={`${color} text-xs`}>{new Date(exp).toLocaleDateString()} ({days}d)</span>;
            }},
            { key: "last_used_day",    label: "Last Used",         render: r => r.last_used_day ? <span className="text-muted-foreground">{new Date(r.last_used_day).toLocaleDateString()}</span> : <span className="text-muted-foreground/40">Never</span> },
          ],
        },
        {
          title: "Secret Scopes",
          resourceType: "secret_scopes",
          emptyMsg: "No secret scopes configured. Use secret scopes to store credentials securely.",
          getLink: (_r, ws) => `${ws}/settings/workspace/secrets`,
          columns: [
            { key: "name",           label: "Scope Name",  render: r => <span className="font-medium">{r.name}</span> },
            { key: "backend_type",   label: "Backend",     render: r => <span className="text-muted-foreground">{r.backend_type ?? "DATABRICKS"}</span> },
          ],
        },
        {
          title: "Git Credentials",
          resourceType: "git_credentials",
          emptyMsg: "No Git credentials configured.",
          columns: [
            { key: "credential_id", label: "Credential ID", width: "110px", render: r => <span className="font-mono text-[10px] text-muted-foreground">{r.credential_id}</span> },
            { key: "git_username",  label: "Git Username",                  render: r => <span className="font-medium">{r.git_username ?? "—"}</span> },
            { key: "git_provider",  label: "Provider",                      render: r => <span className="text-muted-foreground">{r.git_provider ?? "—"}</span> },
          ],
        },
      ]}
    />
  );
}
