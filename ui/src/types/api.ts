/**
 * Shared API types matching FastAPI Pydantic models.
 */

export interface AuthStatus {
  authenticated: boolean;
  user: string | null;
  host: string | null;
  auth_method: string | null;
}

export interface WarehouseInfo {
  id: string;
  name: string;
  size: string;
  state: string;
  type: string;
}

export interface CloneRequest {
  source_catalog: string;
  destination_catalog: string;
  warehouse_id?: string;
  clone_type: "DEEP" | "SHALLOW";
  load_type: "FULL" | "INCREMENTAL";
  dry_run: boolean;
  max_workers: number;
  parallel_tables: number;
  include_schemas: string[];
  exclude_schemas: string[];
  copy_permissions: boolean;
  copy_ownership: boolean;
  copy_tags: boolean;
  copy_properties: boolean;
  copy_security: boolean;
  copy_constraints: boolean;
  copy_comments: boolean;
  enable_rollback: boolean;
  validate_after_clone: boolean;
  order_by_size?: "asc" | "desc";
  location?: string;
}

export interface CloneJob {
  job_id: string;
  status: string;
  source_catalog?: string;
  destination_catalog?: string;
  clone_type?: string;
  progress?: Record<string, unknown>;
  result?: Record<string, unknown>;
  error?: string;
  created_at?: string;
  started_at?: string;
  completed_at?: string;
}

export interface DiffResult {
  schemas: {
    only_in_source: string[];
    only_in_dest: string[];
    in_both: string[];
  };
  tables: {
    only_in_source: string[];
    only_in_dest: string[];
    in_both: string[];
  };
}

export interface PreflightResult {
  ready: boolean;
  checks: Array<{
    name: string;
    status: string;
    message: string;
  }>;
}
