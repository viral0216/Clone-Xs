"""Web dashboard for the catalog clone utility using Streamlit."""

import logging
import os
import sys

logger = logging.getLogger(__name__)


def launch_dashboard(config_path: str = "config/clone_config.yaml", port: int = 8501):
    """Launch the Streamlit web dashboard."""
    try:
        import streamlit  # noqa: F401
    except ImportError:
        logger.error(
            "Streamlit is required for the web dashboard. "
            "Install it with: pip install streamlit"
        )
        sys.exit(1)

    dashboard_script = os.path.join(os.path.dirname(__file__), "_dashboard_app.py")
    if not os.path.exists(dashboard_script):
        logger.error(f"Dashboard app not found at {dashboard_script}")
        sys.exit(1)

    os.environ["CLONE_UTIL_CONFIG"] = config_path
    os.system(f"streamlit run {dashboard_script} --server.port {port}")


def create_dashboard_app_script() -> str:
    """Return the Streamlit app script content (used for generating the file)."""
    return _DASHBOARD_APP_CODE


_DASHBOARD_APP_CODE = '''
import json
import os
import sys
import time

import streamlit as st

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.client import execute_sql, get_workspace_client
from src.config import load_config

st.set_page_config(page_title="Catalog Clone Utility", page_icon="📦", layout="wide")

# --- Sidebar ---
st.sidebar.title("⚙️ Configuration")
config_path = st.sidebar.text_input(
    "Config file", value=os.environ.get("CLONE_UTIL_CONFIG", "config/clone_config.yaml")
)

try:
    config = load_config(config_path)
    config_loaded = True
except Exception as e:
    st.sidebar.error(f"Failed to load config: {e}")
    config = {}
    config_loaded = False

if config_loaded:
    source_catalog = st.sidebar.text_input("Source Catalog", value=config.get("source_catalog", ""))
    dest_catalog = st.sidebar.text_input("Destination Catalog", value=config.get("destination_catalog", ""))
    warehouse_id = st.sidebar.text_input("Warehouse ID", value=config.get("sql_warehouse_id", ""))
    clone_type = st.sidebar.selectbox("Clone Type", ["DEEP", "SHALLOW"], index=0 if config.get("clone_type", "DEEP") == "DEEP" else 1)
    dry_run = st.sidebar.checkbox("Dry Run", value=True)

# --- Main content ---
st.title("📦 Unity Catalog Clone Utility")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "🏠 Overview", "🚀 Clone", "🔍 Diff & Validate",
    "📊 Stats & Profile", "🔎 Search", "⚕️ Preflight", "🛠️ Clone Builder"
])

# --- Tab 1: Overview ---
with tab1:
    st.header("Dashboard Overview")
    if config_loaded:
        col1, col2, col3 = st.columns(3)
        col1.metric("Source", source_catalog)
        col2.metric("Destination", dest_catalog)
        col3.metric("Clone Type", clone_type)

        st.subheader("Current Configuration")
        st.json(config)
    else:
        st.warning("Load a valid config file to get started.")

# --- Tab 2: Clone ---
with tab2:
    st.header("Clone Catalog")
    if config_loaded:
        col1, col2 = st.columns(2)
        with col1:
            include_schemas = st.text_input("Include schemas (comma-separated)", "")
            max_workers = st.slider("Max workers", 1, 16, config.get("max_workers", 4))
        with col2:
            copy_permissions = st.checkbox("Copy permissions", value=config.get("copy_permissions", True))
            copy_tags = st.checkbox("Copy tags", value=config.get("copy_tags", True))
            enable_rollback = st.checkbox("Enable rollback", value=True)

        if st.button("🚀 Start Clone", type="primary"):
            clone_config = {
                **config,
                "source_catalog": source_catalog,
                "destination_catalog": dest_catalog,
                "sql_warehouse_id": warehouse_id,
                "clone_type": clone_type,
                "dry_run": dry_run,
                "max_workers": max_workers,
                "copy_permissions": copy_permissions,
                "copy_tags": copy_tags,
                "enable_rollback": enable_rollback,
            }
            if include_schemas:
                clone_config["include_schemas"] = [s.strip() for s in include_schemas.split(",")]

            with st.spinner("Cloning catalog..." if not dry_run else "Running dry run..."):
                try:
                    from src.clone_catalog import clone_catalog
                    client = get_workspace_client()
                    summary = clone_catalog(client, clone_config)
                    st.success("Clone completed!")
                    st.json(summary)
                except Exception as e:
                    st.error(f"Clone failed: {e}")

# --- Tab 3: Diff & Validate ---
with tab3:
    st.header("Diff & Validate")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Catalog Diff")
        if st.button("🔍 Run Diff"):
            with st.spinner("Comparing catalogs..."):
                try:
                    from src.diff import compare_catalogs
                    client = get_workspace_client()
                    diff = compare_catalogs(
                        client, warehouse_id, source_catalog, dest_catalog,
                        config.get("exclude_schemas", []),
                    )
                    if diff["in_sync"]:
                        st.success("✅ Catalogs are in sync!")
                    else:
                        st.warning("⚠️ Differences found")
                        for dtype in ["tables", "views", "functions"]:
                            only_src = diff.get(f"{dtype}_only_in_source", [])
                            only_dst = diff.get(f"{dtype}_only_in_dest", [])
                            if only_src:
                                st.write(f"**{dtype.title()} only in source:** {len(only_src)}")
                                st.dataframe(only_src)
                            if only_dst:
                                st.write(f"**{dtype.title()} only in dest:** {len(only_dst)}")
                                st.dataframe(only_dst)
                except Exception as e:
                    st.error(f"Diff failed: {e}")

    with col2:
        st.subheader("Validate Row Counts")
        use_checksum = st.checkbox("Use checksum validation")
        if st.button("✅ Run Validation"):
            with st.spinner("Validating..."):
                try:
                    from src.validation import validate_catalog
                    client = get_workspace_client()
                    summary = validate_catalog(
                        client, warehouse_id, source_catalog, dest_catalog,
                        config.get("exclude_schemas", []),
                        config.get("max_workers", 4),
                        use_checksum=use_checksum,
                    )
                    if summary["mismatched"] == 0 and summary["errors"] == 0:
                        st.success(f"✅ All {summary['matched']} tables match!")
                    else:
                        st.error(f"❌ {summary['mismatched']} mismatches, {summary['errors']} errors")
                    st.json(summary)
                except Exception as e:
                    st.error(f"Validation failed: {e}")

# --- Tab 4: Stats & Profile ---
with tab4:
    st.header("Catalog Statistics & Profiling")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Catalog Stats")
        if st.button("📊 Get Stats"):
            with st.spinner("Gathering stats..."):
                try:
                    from src.stats import get_catalog_stats_data
                    client = get_workspace_client()
                    stats = get_catalog_stats_data(
                        client, warehouse_id, source_catalog,
                        config.get("exclude_schemas", []),
                    )
                    st.json(stats)
                except Exception as e:
                    st.error(f"Stats failed: {e}")

    with col2:
        st.subheader("Data Profiling")
        profile_table = st.text_input("Table to profile (schema.table)", "")
        if st.button("🔬 Profile Table") and profile_table:
            with st.spinner("Profiling..."):
                try:
                    from src.profiling import profile_table as do_profile
                    client = get_workspace_client()
                    parts = profile_table.split(".")
                    if len(parts) == 2:
                        result = do_profile(
                            client, warehouse_id, source_catalog, parts[0], parts[1],
                        )
                        st.json(result)
                    else:
                        st.error("Format: schema_name.table_name")
                except Exception as e:
                    st.error(f"Profiling failed: {e}")

# --- Tab 5: Search ---
with tab5:
    st.header("Search Catalog")
    search_pattern = st.text_input("Search pattern (regex)", "")
    search_columns = st.checkbox("Also search column names")
    if st.button("🔎 Search") and search_pattern:
        with st.spinner("Searching..."):
            try:
                from src.search import search_tables_data
                client = get_workspace_client()
                results = search_tables_data(
                    client, warehouse_id, source_catalog,
                    search_pattern, config.get("exclude_schemas", []),
                    search_columns=search_columns,
                )
                if results["matched_tables"]:
                    st.write(f"**Matched tables:** {len(results['matched_tables'])}")
                    st.dataframe(results["matched_tables"])
                if results.get("matched_columns"):
                    st.write(f"**Matched columns:** {len(results['matched_columns'])}")
                    st.dataframe(results["matched_columns"])
                if not results["matched_tables"] and not results.get("matched_columns"):
                    st.info("No matches found.")
            except Exception as e:
                st.error(f"Search failed: {e}")

# --- Tab 6: Preflight ---
with tab6:
    st.header("Pre-Flight Checks")
    if st.button("⚕️ Run Preflight"):
        with st.spinner("Running checks..."):
            try:
                from src.preflight import run_preflight
                client = get_workspace_client()
                result = run_preflight(
                    client, warehouse_id, source_catalog, dest_catalog,
                )
                if result["ready"]:
                    st.success("✅ All checks passed! Ready to clone.")
                else:
                    st.warning("⚠️ Some checks failed.")

                for check in result.get("checks", []):
                    icon = "✅" if check["status"] == "passed" else "⚠️" if check["status"] == "warning" else "❌"
                    st.write(f"{icon} **{check['name']}**: {check.get('message', check['status'])}")
            except Exception as e:
                st.error(f"Preflight failed: {e}")

# --- Tab 7: Clone Builder ---
with tab7:
    st.header("Interactive Clone Builder")
    st.markdown("Build a clone configuration interactively, estimate costs, and execute or export.")

    if config_loaded:
        builder_col1, builder_col2 = st.columns(2)

        with builder_col1:
            st.subheader("1. Select Schemas & Tables")
            st.markdown("Fetch available schemas from the source catalog, then pick what to clone.")

            if st.button("🔄 Fetch Schemas", key="builder_fetch"):
                with st.spinner("Fetching schemas..."):
                    try:
                        client = get_workspace_client()
                        from src.client import execute_sql
                        rows = execute_sql(
                            client, warehouse_id,
                            f"SHOW SCHEMAS IN `{source_catalog}`",
                        )
                        schema_names = [r["databaseName"] for r in rows]
                        schema_names = [
                            s for s in schema_names
                            if s not in ("information_schema", "default")
                        ]
                        st.session_state["builder_schemas"] = schema_names
                    except Exception as e:
                        st.error(f"Failed to fetch schemas: {e}")
                        st.session_state["builder_schemas"] = []

            available_schemas = st.session_state.get("builder_schemas", [])
            selected_schemas = st.multiselect(
                "Schemas to clone",
                options=available_schemas,
                default=available_schemas,
                key="builder_selected_schemas",
            )

            st.markdown("**Table filtering** (optional)")
            include_tables_input = st.text_input(
                "Include tables (comma-separated patterns, e.g. 'dim_*,fact_*')",
                key="builder_include_tables",
            )
            exclude_tables_input = st.text_input(
                "Exclude tables (comma-separated patterns, e.g. 'tmp_*,staging_*')",
                key="builder_exclude_tables",
            )

        with builder_col2:
            st.subheader("2. Clone Options")

            builder_clone_type = st.selectbox(
                "Clone type",
                ["DEEP", "SHALLOW"],
                index=0,
                key="builder_clone_type",
            )
            builder_dest = st.text_input(
                "Destination catalog",
                value=dest_catalog,
                key="builder_dest_catalog",
            )
            builder_workers = st.slider(
                "Max parallel workers", 1, 16, 4, key="builder_workers"
            )
            builder_copy_perms = st.checkbox(
                "Copy permissions", value=True, key="builder_copy_perms"
            )
            builder_copy_tags = st.checkbox(
                "Copy tags", value=True, key="builder_copy_tags"
            )
            builder_enable_rollback = st.checkbox(
                "Enable rollback on failure", value=True, key="builder_rollback"
            )
            builder_dry_run = st.checkbox(
                "Dry run (preview only)", value=True, key="builder_dry_run"
            )

        st.markdown("---")

        builder_config = {
            "source_catalog": source_catalog,
            "destination_catalog": builder_dest,
            "sql_warehouse_id": warehouse_id,
            "clone_type": builder_clone_type,
            "dry_run": builder_dry_run,
            "max_workers": builder_workers,
            "copy_permissions": builder_copy_perms,
            "copy_tags": builder_copy_tags,
            "enable_rollback": builder_enable_rollback,
        }
        if selected_schemas:
            builder_config["include_schemas"] = selected_schemas
        if include_tables_input.strip():
            builder_config["include_tables"] = [
                t.strip() for t in include_tables_input.split(",") if t.strip()
            ]
        if exclude_tables_input.strip():
            builder_config["exclude_tables"] = [
                t.strip() for t in exclude_tables_input.split(",") if t.strip()
            ]

        cost_col, export_col = st.columns(2)

        with cost_col:
            st.subheader("3. Cost Estimation")
            if st.button("💰 Estimate Cost", key="builder_estimate"):
                with st.spinner("Estimating..."):
                    try:
                        from src.cost_estimation import estimate_clone_cost
                        client = get_workspace_client()
                        estimate = estimate_clone_cost(
                            client, warehouse_id, source_catalog,
                            selected_schemas or [],
                            builder_clone_type,
                        )
                        st.session_state["builder_estimate"] = estimate
                    except Exception as e:
                        st.warning(f"Cost estimation unavailable: {e}")
                        st.session_state["builder_estimate"] = None

            estimate = st.session_state.get("builder_estimate")
            if estimate:
                st.metric("Estimated Tables", estimate.get("total_tables", "N/A"))
                st.metric("Estimated Size", estimate.get("total_size_display", "N/A"))
                st.metric("Est. Duration", estimate.get("estimated_duration", "N/A"))
                if estimate.get("details"):
                    with st.expander("Cost details"):
                        st.json(estimate)

        with export_col:
            st.subheader("4. Export Config")
            st.markdown("Download this configuration as a YAML file.")

            try:
                import yaml
                yaml_str = yaml.dump(builder_config, default_flow_style=False, sort_keys=False)
            except ImportError:
                yaml_lines = []
                for k, v in builder_config.items():
                    if isinstance(v, list):
                        yaml_lines.append(f"{k}:")
                        for item in v:
                            yaml_lines.append(f"  - {item}")
                    elif isinstance(v, bool):
                        yaml_lines.append(f"{k}: {'true' if v else 'false'}")
                    else:
                        yaml_lines.append(f"{k}: {v}")
                yaml_str = "\\n".join(yaml_lines)

            st.code(yaml_str, language="yaml")
            st.download_button(
                label="📥 Download YAML",
                data=yaml_str,
                file_name="clone_config.yaml",
                mime="text/yaml",
                key="builder_download",
            )

        st.markdown("---")

        st.subheader("5. Execute Clone")
        st.warning(
            "Review your configuration above before executing. "
            "Uncheck 'Dry run' to perform actual cloning."
        )
        if st.button("🚀 Execute Clone", type="primary", key="builder_execute"):
            action = "dry run" if builder_dry_run else "clone"
            with st.spinner(f"Running {action}..."):
                try:
                    from src.clone_catalog import clone_catalog
                    client = get_workspace_client()
                    summary = clone_catalog(client, builder_config)
                    st.success(f"{'Dry run' if builder_dry_run else 'Clone'} completed!")
                    st.json(summary)
                except Exception as e:
                    st.error(f"Clone failed: {e}")
    else:
        st.warning("Load a valid config file to use the Clone Builder.")

st.sidebar.markdown("---")
st.sidebar.markdown("**Clone-Xs** v0.5.0")
st.sidebar.markdown("[Documentation](./HOWTO.md) | [GitHub](#)")
'''
