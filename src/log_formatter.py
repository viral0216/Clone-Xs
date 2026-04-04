"""ANSI color-coded logging formatter and display helpers for Clone-Xs.

Provides colored log output for terminals and styled summary/header helpers.
Colors are automatically disabled when output is not a TTY (e.g., piped to file).
"""

import logging
import sys


# ── ANSI escape codes ─────────────────────────────────────────────
_IS_TTY = sys.stderr.isatty()


def _c(code: str, text: str) -> str:
    """Wrap text in ANSI color code if output is a TTY."""
    if not _IS_TTY:
        return text
    return f"\033[{code}m{text}\033[0m"


# Colors
def gray(t: str) -> str:    return _c("90", t)
def red(t: str) -> str:     return _c("31", t)
def green(t: str) -> str:   return _c("32", t)
def yellow(t: str) -> str:  return _c("33", t)
def blue(t: str) -> str:    return _c("34", t)
def cyan(t: str) -> str:    return _c("36", t)
def bold(t: str) -> str:    return _c("1", t)
def dim(t: str) -> str:     return _c("2", t)

# Bold + color
def bold_green(t: str) -> str:  return _c("1;32", t)
def bold_red(t: str) -> str:    return _c("1;31", t)
def bold_yellow(t: str) -> str: return _c("1;33", t)
def bold_cyan(t: str) -> str:   return _c("1;36", t)
def bold_blue(t: str) -> str:   return _c("1;34", t)


# ── Icons ─────────────────────────────────────────────────────────
OK      = green("✓")
FAIL    = red("✗")
SKIP    = yellow("⊘")
INFO    = blue("ℹ")
WARN    = yellow("⚠")
ERR     = red("✗")
ARROW   = cyan("→")
TABLE   = "◫"
VIEW    = "◧"
FUNC    = "ƒ"
VOL     = "▤"
SCHEMA  = "◈"
CATALOG = "▣"
LOCK    = "⛨"
CLOCK   = dim("⏱")


# ── Logging formatter ────────────────────────────────────────────

_LEVEL_COLORS = {
    "DEBUG":    "90",      # gray
    "INFO":     "36",      # cyan
    "WARNING":  "33",      # yellow
    "ERROR":    "1;31",    # bold red
    "CRITICAL": "1;31;41", # bold red on red bg
}

_LEVEL_LABELS = {
    "DEBUG":    " DBG ",
    "INFO":     " INF ",
    "WARNING":  " WRN ",
    "ERROR":    " ERR ",
    "CRITICAL": " CRT ",
}


class ColorFormatter(logging.Formatter):
    """Logging formatter with ANSI colors for terminal output.

    Falls back to plain text when stderr is not a TTY.
    """

    def __init__(self, datefmt: str = "%H:%M:%S"):
        super().__init__(datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        level = record.levelname
        label = _LEVEL_LABELS.get(level, f" {level[:3]} ")
        name = record.name

        # Shorten module names: src.clone_catalog -> clone_catalog
        if name.startswith("src."):
            name = name[4:]

        msg = record.getMessage()

        if _IS_TTY:
            color = _LEVEL_COLORS.get(level, "0")
            ts_str = f"\033[90m{ts}\033[0m"
            level_str = f"\033[{color}m{label}\033[0m"
            name_str = f"\033[90m{name}\033[0m"
            return f"{ts_str} {level_str} {name_str}: {msg}"
        else:
            return f"{ts} [{level}] {name}: {msg}"


def setup_color_logging(verbose: bool = False, log_file: str | None = None) -> None:
    """Configure logging with colored console output.

    Args:
        verbose: If True, set DEBUG level; otherwise INFO.
        log_file: Optional file path for plain-text log output.
    """
    level = logging.DEBUG if verbose else logging.INFO

    console = logging.StreamHandler()
    console.setFormatter(ColorFormatter())
    handlers: list[logging.Handler] = [console]

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers, force=True)


# ── Display helpers ──────────────────────────────────────────────

def header(title: str, width: int = 60) -> str:
    """Render a styled section header."""
    line = "─" * width
    if _IS_TTY:
        return f"\033[1;36m┌{line}┐\033[0m\n\033[1;36m│\033[0m \033[1m{title:<{width - 2}}\033[0m \033[1;36m│\033[0m\n\033[1;36m└{line}┘\033[0m"
    else:
        return f"{'=' * width}\n{title}\n{'=' * width}"


def divider(width: int = 60) -> str:
    """Render a horizontal divider."""
    if _IS_TTY:
        return f"\033[90m{'─' * width}\033[0m"
    else:
        return "-" * width


def stat_line(label: str, success: int, failed: int, skipped: int) -> str:
    """Render a stats line for an object type (tables, views, etc.)."""
    parts = []
    if success:
        parts.append(f"{OK} {bold_green(str(success))} success")
    else:
        parts.append(f"  {dim('0')} success")
    if failed:
        parts.append(f"{FAIL} {bold_red(str(failed))} failed")
    else:
        parts.append(f"  {dim('0')} failed")
    if skipped:
        parts.append(f"{SKIP} {yellow(str(skipped))} skipped")
    else:
        parts.append(f"  {dim('0')} skipped")

    return f"  {label:12s}  {', '.join(parts)}"


def kv(key: str, value: str) -> str:
    """Render a key-value pair."""
    return f"  {dim(key + ':')} {value}"
