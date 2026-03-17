"""Interactive TUI (Terminal User Interface) using Rich library."""

import logging
import sys

logger = logging.getLogger(__name__)


def run_tui(config_path: str = "config/clone_config.yaml"):
    """Launch the interactive TUI for the catalog clone utility."""
    try:
        from rich.console import Console
        from rich.panel import Panel
        from rich.prompt import Confirm, IntPrompt, Prompt
        from rich.table import Table
        from rich.tree import Tree
    except ImportError:
        logger.error("Rich is required for the TUI. Install with: pip install rich")
        sys.exit(1)

    console = Console()

    from src.client import get_workspace_client
    from src.config import load_config

    console.print(Panel.fit(
        "[bold blue]Unity Catalog Clone Utility[/bold blue]\n"
        "[dim]Interactive Terminal Interface[/dim]",
        border_style="blue",
    ))

    # Load config
    try:
        config = load_config(config_path)
        console.print(f"[green]✓[/green] Config loaded: {config_path}")
    except Exception as e:
        console.print(f"[red]✗[/red] Config error: {e}")
        config_path = Prompt.ask("Enter config path", default="config/clone_config.yaml")
        config = load_config(config_path)

    # Show current config
    config_table = Table(title="Current Configuration", show_header=True)
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")
    config_table.add_row("Source Catalog", config.get("source_catalog", "N/A"))
    config_table.add_row("Destination Catalog", config.get("destination_catalog", "N/A"))
    config_table.add_row("Clone Type", config.get("clone_type", "DEEP"))
    config_table.add_row("Warehouse ID", config.get("sql_warehouse_id", "N/A"))
    config_table.add_row("Max Workers", str(config.get("max_workers", 4)))
    console.print(config_table)
    console.print()

    # Main menu loop
    while True:
        console.print("[bold]Available Operations:[/bold]")
        menu_table = Table(show_header=False, box=None)
        menu_table.add_column("Key", style="bold yellow", width=4)
        menu_table.add_column("Action")

        menu_items = [
            ("1", "Clone catalog"),
            ("2", "Diff catalogs"),
            ("3", "Validate clone"),
            ("4", "Pre-flight checks"),
            ("5", "Catalog stats"),
            ("6", "Search tables"),
            ("7", "PII scan"),
            ("8", "Schema evolution check"),
            ("9", "Dependency graph"),
            ("10", "Cost estimate"),
            ("11", "Audit history"),
            ("q", "Quit"),
        ]
        for key, action in menu_items:
            menu_table.add_row(key, action)
        console.print(menu_table)

        choice = Prompt.ask("\nSelect operation", choices=[str(i) for i in range(1, 12)] + ["q"])

        if choice == "q":
            console.print("[blue]Goodbye![/blue]")
            break

        source = Prompt.ask("Source catalog", default=config.get("source_catalog", ""))
        dest = Prompt.ask("Destination catalog", default=config.get("destination_catalog", ""))
        warehouse_id = config.get("sql_warehouse_id", "")

        try:
            client = get_workspace_client()
        except Exception as e:
            console.print(f"[red]Failed to connect: {e}[/red]")
            continue

        if choice == "1":
            _tui_clone(console, client, config, source, dest, warehouse_id)
        elif choice == "2":
            _tui_diff(console, client, config, source, dest, warehouse_id)
        elif choice == "3":
            _tui_validate(console, client, config, source, dest, warehouse_id)
        elif choice == "4":
            _tui_preflight(console, client, source, dest, warehouse_id)
        elif choice == "5":
            _tui_stats(console, client, source, warehouse_id, config)
        elif choice == "6":
            _tui_search(console, client, source, warehouse_id, config)
        elif choice == "7":
            _tui_pii_scan(console, client, source, warehouse_id, config)
        elif choice == "8":
            _tui_schema_evolution(console, client, source, dest, warehouse_id, config)
        elif choice == "9":
            _tui_dependency_graph(console, client, source, warehouse_id)
        elif choice == "10":
            _tui_cost_estimate(console, client, source, warehouse_id, config)
        elif choice == "11":
            _tui_audit_history(console, client, warehouse_id, config)

        console.print()


def _tui_clone(console, client, config, source, dest, warehouse_id):
    """TUI clone operation."""
    from rich.prompt import Confirm, Prompt

    clone_type = Prompt.ask("Clone type", choices=["DEEP", "SHALLOW"], default="DEEP")
    dry_run = Confirm.ask("Dry run first?", default=True)

    clone_config = {
        **config,
        "source_catalog": source,
        "destination_catalog": dest,
        "sql_warehouse_id": warehouse_id,
        "clone_type": clone_type,
        "dry_run": dry_run,
    }

    with console.status("[bold green]Cloning catalog..."):
        from src.clone_catalog import clone_catalog
        summary = clone_catalog(client, clone_config)

    _print_clone_summary(console, summary)


def _tui_diff(console, client, config, source, dest, warehouse_id):
    """TUI diff operation."""
    with console.status("[bold green]Comparing catalogs..."):
        from src.diff import compare_catalogs
        diff = compare_catalogs(
            client, warehouse_id, source, dest, config.get("exclude_schemas", [])
        )

    if diff.get("in_sync"):
        console.print("[bold green]✓ Catalogs are in sync![/bold green]")
    else:
        from rich.table import Table
        for obj_type in ["tables", "views", "functions"]:
            only_src = diff.get(f"{obj_type}_only_in_source", [])
            only_dst = diff.get(f"{obj_type}_only_in_dest", [])
            if only_src or only_dst:
                t = Table(title=f"{obj_type.title()} Differences")
                t.add_column("Only in Source", style="red")
                t.add_column("Only in Dest", style="yellow")
                max_len = max(len(only_src), len(only_dst))
                for i in range(max_len):
                    s = only_src[i] if i < len(only_src) else ""
                    d = only_dst[i] if i < len(only_dst) else ""
                    t.add_row(s, d)
                console.print(t)


def _tui_validate(console, client, config, source, dest, warehouse_id):
    """TUI validate operation."""
    from rich.prompt import Confirm
    use_checksum = Confirm.ask("Use checksum validation?", default=False)

    with console.status("[bold green]Validating..."):
        from src.validation import validate_catalog
        summary = validate_catalog(
            client, warehouse_id, source, dest,
            config.get("exclude_schemas", []), config.get("max_workers", 4),
            use_checksum=use_checksum,
        )

    if summary["mismatched"] == 0 and summary["errors"] == 0:
        console.print(f"[bold green]✓ All {summary['matched']} tables match![/bold green]")
    else:
        console.print(f"[bold red]✗ {summary['mismatched']} mismatches, {summary['errors']} errors[/bold red]")


def _tui_preflight(console, client, source, dest, warehouse_id):
    """TUI preflight operation."""
    from rich.table import Table

    with console.status("[bold green]Running pre-flight checks..."):
        from src.preflight import run_preflight
        result = run_preflight(client, warehouse_id, source, dest)

    t = Table(title="Pre-Flight Results")
    t.add_column("Check", style="cyan")
    t.add_column("Status")
    t.add_column("Message")

    for check in result.get("checks", []):
        status_style = {"passed": "green", "warning": "yellow", "failed": "red"}.get(check["status"], "white")
        t.add_row(check["name"], f"[{status_style}]{check['status']}[/{status_style}]", check.get("message", ""))

    console.print(t)


def _tui_stats(console, client, source, warehouse_id, config):
    """TUI stats operation."""
    with console.status("[bold green]Gathering stats..."):
        from src.stats import catalog_stats
        catalog_stats(client, warehouse_id, source, config.get("exclude_schemas", []))


def _tui_search(console, client, source, warehouse_id, config):
    """TUI search operation."""
    from rich.prompt import Confirm, Prompt
    pattern = Prompt.ask("Search pattern (regex)")
    search_cols = Confirm.ask("Search column names too?", default=False)

    with console.status("[bold green]Searching..."):
        from src.search import search_tables
        search_tables(
            client, warehouse_id, source, pattern,
            config.get("exclude_schemas", []), search_columns=search_cols,
        )


def _tui_pii_scan(console, client, source, warehouse_id, config):
    """TUI PII scan operation."""
    from rich.prompt import Confirm

    sample_data = Confirm.ask("Sample actual data values? (slower but more accurate)", default=False)
    read_uc_tags = Confirm.ask("Read Unity Catalog tags to enhance detection?", default=False)

    with console.status("[bold green]Scanning for PII..."):
        from src.pii_detection import scan_catalog_for_pii
        result = scan_catalog_for_pii(
            client, warehouse_id, source,
            config.get("exclude_schemas", []),
            sample_data=sample_data,
            pii_config=config.get("pii_detection"),
            read_uc_tags=read_uc_tags,
        )

    console.print(f"Found [bold]{result['summary']['pii_columns_found']}[/bold] potential PII columns")

    if result["summary"]["pii_columns_found"] > 0:
        apply_tags = Confirm.ask("Apply PII tags to Unity Catalog?", default=False)
        if apply_tags:
            from src.pii_tagging import apply_pii_tags
            with console.status("[bold green]Applying PII tags..."):
                tag_result = apply_pii_tags(
                    client, warehouse_id, source,
                    result.get("columns", []),
                )
            console.print(
                f"Tagged [bold]{tag_result['tagged']}[/bold] columns, "
                f"skipped {tag_result['skipped']}, errors {tag_result['errors']}"
            )


def _tui_schema_evolution(console, client, source, dest, warehouse_id, config):
    """TUI schema evolution check."""
    from rich.prompt import Confirm

    dry_run = Confirm.ask("Dry run (preview only)?", default=True)

    with console.status("[bold green]Checking schema evolution..."):
        from src.schema_evolution import evolve_catalog_schema
        result = evolve_catalog_schema(
            client, warehouse_id, source, dest,
            config.get("exclude_schemas", []),
            dry_run=dry_run,
        )

    console.print(f"Tables with changes: [bold]{result['tables_with_changes']}[/bold]")


def _tui_dependency_graph(console, client, source, warehouse_id):
    """TUI dependency graph."""
    with console.status("[bold green]Building dependency graph..."):
        from src.dependency_graph import build_dependency_graph, print_dependency_graph
        graph = build_dependency_graph(client, warehouse_id, source)
        print_dependency_graph(graph)


def _tui_cost_estimate(console, client, source, warehouse_id, config):
    """TUI cost estimate."""
    with console.status("[bold green]Estimating costs..."):
        from src.clone_cost_estimator import estimate_clone_cost
        result = estimate_clone_cost(
            client, warehouse_id, source,
            config.get("exclude_schemas", []),
            clone_type=config.get("clone_type", "DEEP"),
        )

    console.print(f"Estimated cost: [bold green]${result['cost_estimate']['one_time_compute_cost_usd']:.2f}[/bold green] compute + [bold green]${result['cost_estimate']['monthly_storage_cost_usd']:.2f}/mo[/bold green] storage")


def _tui_audit_history(console, client, warehouse_id, config):
    """TUI audit history."""
    with console.status("[bold green]Querying audit history..."):
        from src.audit_trail import query_audit_history
        query_audit_history(client, warehouse_id, config)


def _print_clone_summary(console, summary):
    """Print clone summary in rich format."""
    from rich.table import Table

    t = Table(title="Clone Summary")
    t.add_column("Object Type", style="cyan")
    t.add_column("Cloned", style="green")
    t.add_column("Failed", style="red")
    t.add_column("Skipped", style="yellow")

    for obj_type in ("tables", "views", "functions", "volumes"):
        info = summary.get(obj_type, {})
        t.add_row(
            obj_type.title(),
            str(info.get("cloned", 0)),
            str(info.get("failed", 0)),
            str(info.get("skipped", 0)),
        )

    console.print(t)
