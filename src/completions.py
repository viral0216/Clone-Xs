import sys


def generate_bash_completion() -> str:
    """Generate bash completion script for clxs CLI."""
    return '''# Bash completion for clxs
# Add to ~/.bashrc: eval "$(clxs --completion bash)"
# Or save to: /etc/bash_completion.d/clxs

_clone_catalog_completion() {
    local cur prev commands opts

    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    commands="clone diff compare rollback validate estimate generate-workflow sync snapshot schema-drift init export-iac preflight search stats profile monitor export"

    # Common options
    common_opts="-c --config --warehouse-id -v --verbose --profile --log-file -h --help"

    # Command-specific options
    case "${COMP_WORDS[1]}" in
        clone)
            opts="$common_opts --source --dest --clone-type --load-type --max-workers --no-permissions --no-ownership --no-tags --no-properties --no-security --no-constraints --no-comments --dry-run --include-schemas --report --enable-rollback --validate --checksum --parallel-tables --include-tables-regex --exclude-tables-regex --resume --progress --no-progress --order-by-size --max-rps --dest-host --dest-token --dest-warehouse-id --as-of-timestamp --as-of-version"
            ;;
        diff|compare|schema-drift)
            opts="$common_opts --source --dest"
            ;;
        rollback)
            opts="$common_opts --log-file --list --drop-catalog"
            ;;
        validate)
            opts="$common_opts --source --dest --checksum"
            ;;
        estimate)
            opts="$common_opts --source --price-per-gb"
            ;;
        generate-workflow)
            opts="$common_opts --format --output --job-name --cluster-id --schedule --notification-email"
            ;;
        sync)
            opts="$common_opts --source --dest --dry-run --drop-extra"
            ;;
        snapshot)
            opts="$common_opts --source --output"
            ;;
        init)
            opts="--output -h --help"
            ;;
        export-iac)
            opts="$common_opts --source --format --output"
            ;;
        preflight)
            opts="$common_opts --source --dest --no-write-check"
            ;;
        search)
            opts="$common_opts --source --pattern --columns"
            ;;
        stats)
            opts="$common_opts --source"
            ;;
        profile)
            opts="$common_opts --source --output"
            ;;
        monitor)
            opts="$common_opts --source --dest --interval --max-checks --check-drift --check-counts"
            ;;
        export)
            opts="$common_opts --source --format --output"
            ;;
        *)
            COMPREPLY=( $(compgen -W "${commands}" -- ${cur}) )
            return 0
            ;;
    esac

    case "${prev}" in
        --clone-type)
            COMPREPLY=( $(compgen -W "DEEP SHALLOW" -- ${cur}) )
            return 0
            ;;
        --load-type)
            COMPREPLY=( $(compgen -W "FULL INCREMENTAL" -- ${cur}) )
            return 0
            ;;
        --order-by-size)
            COMPREPLY=( $(compgen -W "asc desc" -- ${cur}) )
            return 0
            ;;
        --format)
            if [[ "${COMP_WORDS[1]}" == "generate-workflow" ]]; then
                COMPREPLY=( $(compgen -W "json yaml" -- ${cur}) )
            elif [[ "${COMP_WORDS[1]}" == "export-iac" ]]; then
                COMPREPLY=( $(compgen -W "terraform pulumi" -- ${cur}) )
            elif [[ "${COMP_WORDS[1]}" == "export" ]]; then
                COMPREPLY=( $(compgen -W "csv json" -- ${cur}) )
            fi
            return 0
            ;;
        -c|--config|--log-file|--resume|--output)
            COMPREPLY=( $(compgen -f -- ${cur}) )
            return 0
            ;;
    esac

    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
    return 0
}

complete -F _clone_catalog_completion clxs
'''


def generate_zsh_completion() -> str:
    """Generate zsh completion script for clxs CLI."""
    return '''#compdef clxs
# Zsh completion for clxs
# Add to ~/.zshrc: eval "$(clxs --completion zsh)"
# Or save to a file in your $fpath

_clxs() {
    local -a commands
    commands=(
        'clone:Clone a catalog from source to destination'
        'diff:Compare source and destination catalogs'
        'compare:Deep column-level comparison of catalogs'
        'rollback:Rollback a previous clone operation'
        'validate:Validate clone by comparing row counts'
        'estimate:Estimate storage cost for a deep clone'
        'generate-workflow:Generate Databricks Workflows job definition'
        'sync:Two-way sync between catalogs'
        'snapshot:Export catalog metadata to JSON'
        'schema-drift:Detect schema drift between catalogs'
        'init:Interactive config wizard'
        'export-iac:Generate Terraform or Pulumi config'
        'preflight:Run pre-flight checks'
        'search:Search for tables and columns'
        'stats:Show catalog statistics'
        'profile:Profile table data quality'
        'monitor:Continuous sync monitoring'
        'export:Export catalog metadata to CSV/JSON'
    )

    _arguments -C \\
        '1:command:->command' \\
        '*::arg:->args'

    case "$state" in
        command)
            _describe 'command' commands
            ;;
    esac
}

_clxs "$@"
'''


def generate_fish_completion() -> str:
    """Generate fish completion script for clxs CLI."""
    commands = {
        "clone": "Clone a catalog from source to destination",
        "diff": "Compare source and destination catalogs",
        "compare": "Deep column-level comparison",
        "rollback": "Rollback a previous clone",
        "validate": "Validate clone by comparing row counts",
        "estimate": "Estimate storage cost",
        "generate-workflow": "Generate Databricks Workflows job",
        "sync": "Two-way sync between catalogs",
        "snapshot": "Export catalog metadata to JSON",
        "schema-drift": "Detect schema drift",
        "init": "Interactive config wizard",
        "export-iac": "Generate Terraform or Pulumi config",
        "preflight": "Run pre-flight checks",
        "search": "Search for tables and columns",
        "stats": "Show catalog statistics",
        "profile": "Profile table data quality",
        "monitor": "Continuous sync monitoring",
        "export": "Export catalog metadata",
    }

    lines = ["# Fish completion for clxs"]
    for cmd, desc in commands.items():
        lines.append(
            f"complete -c clxs -n '__fish_use_subcommand' "
            f"-a '{cmd}' -d '{desc}'"
        )

    return "\n".join(lines) + "\n"


def install_completions(shell: str) -> None:
    """Print completion script to stdout for the given shell."""
    generators = {
        "bash": generate_bash_completion,
        "zsh": generate_zsh_completion,
        "fish": generate_fish_completion,
    }

    if shell not in generators:
        print(f"Unsupported shell: {shell}. Use bash, zsh, or fish.", file=sys.stderr)
        sys.exit(1)

    print(generators[shell]())
