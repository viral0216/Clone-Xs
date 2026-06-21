"""Modern (SchemaX/Docusaurus-style) HTML report for SAT Scanner."""
from __future__ import annotations

import html as _html_mod
import json as _json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ... import __version__
from ...models import SATScanResult, SATFinding
from ...checks import SAT_CHECKS, CHECK_API_ENDPOINTS, CHECK_BENEFITS, CATEGORY_DEFINITIONS, _get_effort
from ...scoring import _build_prioritised_recommendations
from ...helpers import _pl, _sanitize_name, _details_str, _render_secret_details_html, _render_scan_items_html

logger = logging.getLogger(__name__)
_esc = _html_mod.escape

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_MODERN_CSS = """\
/* ── Shared constants ── */
:root {
  --ifm-navbar-height: 3.75rem;
  --ifm-font-family-base: system-ui, -apple-system, "Segoe UI", Roboto, Ubuntu, Cantarell,
    "Noto Sans", sans-serif, BlinkMacSystemFont, Helvetica, Arial, "Apple Color Emoji",
    "Segoe UI Emoji", "Segoe UI Symbol";
  --ifm-font-family-monospace: SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
    "Courier New", monospace;
  --ifm-font-weight-semibold: 500;
  --ifm-font-weight-bold: 700;
  --ifm-line-height-base: 1.65;
  --ifm-border-radius: 0.4rem;
  --ifm-card-border-radius: 0.8rem;
  --ifm-navbar-shadow: 0 1px 2px 0 rgba(0,0,0,.1);
  --doc-sidebar-width: 300px;
  --ifm-color-primary: #FF3621;
  --ifm-color-primary-dark: #E02E1B;
  --ifm-color-primary-light: #FF5744;
  --ifm-color-primary-lightest: #FFD4CF;
  --pass: #00a400; --fail: #fa383e; --warn: #ffba00; --info: #54c7ec;
  --high: #ea580c; --muted: #64748b;
}

/* ── Dark theme (default) ── */
[data-theme="dark"] {
  --ifm-background-color: #1b1b1d;
  --ifm-background-surface-color: #242526;
  --ifm-color-content: #e3e3e3;
  --ifm-color-content-secondary: #dadde1;
  --ifm-color-emphasis-200: #444950;
  --ifm-color-emphasis-300: #606770;
  --ifm-color-emphasis-400: #8d949e;
  --ifm-color-emphasis-600: #ccd0d5;
  --ifm-color-emphasis-700: #dadde1;
  --ifm-color-emphasis-900: #f5f6f7;
  --ifm-hover-overlay: rgba(255,255,255,.05);
  --ifm-footer-bg: #303846;
  --ifm-footer-title-color: #fff;
  --ifm-footer-link-color: #ebedf0;
}

/* ── Light theme ── */
[data-theme="light"] {
  --ifm-background-color: #ffffff;
  --ifm-background-surface-color: #f6f8fa;
  --ifm-color-content: #1c1e21;
  --ifm-color-content-secondary: #474a4e;
  --ifm-color-emphasis-200: #ebedf0;
  --ifm-color-emphasis-300: #dadde1;
  --ifm-color-emphasis-400: #a4a8ad;
  --ifm-color-emphasis-600: #606770;
  --ifm-color-emphasis-700: #444950;
  --ifm-color-emphasis-900: #1c1e21;
  --ifm-hover-overlay: rgba(0,0,0,.03);
  --ifm-footer-bg: #303846;
  --ifm-footer-title-color: #fff;
  --ifm-footer-link-color: #ebedf0;
  --ifm-color-primary: #E02E1B;
  --ifm-color-primary-light: #C4271A;
  --ifm-color-primary-lightest: #FFD4CF;
  --ifm-navbar-shadow: 0 1px 2px 0 rgba(0,0,0,.06);
  --pass: #16a34a; --fail: #dc2626; --warn: #ca8a04; --info: #2563eb;
  --high: #c2410c; --muted: #64748b;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 100%; scroll-behavior: smooth; }
body { font-family: var(--ifm-font-family-base); background: var(--ifm-background-color);
       color: var(--ifm-color-content); line-height: var(--ifm-line-height-base);
       -webkit-font-smoothing: antialiased; }
a { color: var(--ifm-color-primary-light); text-decoration: none; }
a:hover { color: var(--ifm-color-primary-lightest); text-decoration: underline; }

/* ── Navbar (top — Docusaurus-style) ── */
.navbar { position: sticky; top: 0; z-index: 200; height: var(--ifm-navbar-height);
          background-color: var(--ifm-background-surface-color);
          box-shadow: var(--ifm-navbar-shadow); display: flex; align-items: center;
          padding: 0.5rem 1rem; }
.navbar__brand { display: flex; align-items: center; gap: 0.5rem; font-weight: var(--ifm-font-weight-bold);
                 font-size: 1.1rem; color: var(--ifm-color-emphasis-900); text-decoration: none; }
.navbar__brand svg { width: 24px; height: 24px; color: var(--ifm-color-primary); }
.navbar__items { display: flex; align-items: center; gap: 0.5rem; margin-left: auto; }
.navbar__link { color: var(--ifm-color-emphasis-700); font-weight: var(--ifm-font-weight-semibold);
                font-size: 0.875rem; padding: 0.25rem 0.75rem; border-radius: var(--ifm-border-radius);
                transition: color .2s, background .2s; text-decoration: none; }
.navbar__link:hover { color: var(--ifm-color-primary); background: var(--ifm-hover-overlay); text-decoration: none; }
.navbar__link--active { color: var(--ifm-color-primary); }
.navbar__version { font-size: 0.75rem; color: var(--ifm-color-emphasis-400);
                   margin-left: 0.5rem; white-space: nowrap; }
.navbar__search { display: flex; align-items: center; gap: 0.35rem; padding: 0.35rem 0.75rem;
                  border: 1px solid var(--ifm-color-emphasis-200); border-radius: var(--ifm-border-radius);
                  font-size: 0.8rem; color: var(--ifm-color-emphasis-400); background: var(--ifm-background-color);
                  cursor: default; white-space: nowrap; }
.navbar__search svg { width: 14px; height: 14px; opacity: 0.5; }
.navbar__search kbd { font-family: var(--ifm-font-family-monospace); font-size: 0.65rem;
                      background: var(--ifm-color-emphasis-200); padding: 1px 5px; border-radius: 3px;
                      color: var(--ifm-color-emphasis-400); margin-left: 0.5rem; }

/* ── Layout: sidebar + main + toc ── */
.layout { display: flex; min-height: calc(100vh - var(--ifm-navbar-height)); }

/* ── Left Sidebar (Docusaurus-style) ── */
.sidebar { width: var(--doc-sidebar-width); min-width: var(--doc-sidebar-width);
           border-right: 1px solid var(--ifm-color-emphasis-200); overflow-y: auto;
           padding: 0.5rem 0; position: sticky; top: var(--ifm-navbar-height);
           height: calc(100vh - var(--ifm-navbar-height)); background: var(--ifm-background-color);
           scrollbar-width: thin; scrollbar-color: #686868 transparent; }
.sidebar::-webkit-scrollbar { width: 4px; }
.sidebar::-webkit-scrollbar-thumb { background: #686868; border-radius: 4px; }
.menu__list { list-style: none; padding: 0 0.5rem; margin: 0; }
.menu__list-item { margin-bottom: 1px; }
.menu__link { color: var(--ifm-color-emphasis-600); font-size: 0.875rem; line-height: 1.25;
              display: flex; align-items: center; padding: 0.375rem 0.75rem;
              border-radius: 0.25rem; cursor: pointer; border: none; background: none;
              width: 100%; text-align: left; transition: color .15s, background .15s;
              text-decoration: none; gap: 0.5rem;
              border-left: 3px solid transparent; margin-left: -3px; }
.menu__link:hover { background: var(--ifm-hover-overlay); color: var(--ifm-color-emphasis-900); text-decoration: none; }
.menu__link--active { color: var(--ifm-color-primary) !important; background: rgba(255,54,33,.05);
                      border-left-color: var(--ifm-color-primary); font-weight: var(--ifm-font-weight-semibold); }
.menu__badge { margin-left: auto; padding: 1px 8px; border-radius: 10px; font-size: 0.65rem;
               font-weight: 600; background: var(--ifm-color-emphasis-200); color: var(--ifm-color-emphasis-400);
               flex-shrink: 0; }
.menu__link--active .menu__badge { background: rgba(255,54,33,.15); color: var(--ifm-color-primary-light); }
.menu__category { padding: 0.75rem 0.75rem 0.35rem; font-size: 0.8rem; font-weight: var(--ifm-font-weight-bold);
                  color: var(--ifm-color-emphasis-700); display: flex; align-items: center;
                  justify-content: space-between; cursor: default; }
.menu__category-chevron { font-size: 0.6rem; color: var(--ifm-color-emphasis-400); transition: transform .2s; }

/* ── Main content area ── */
.main { flex: 1; min-width: 0; overflow-y: auto; padding: 1.25rem 2.5rem 2rem; }

/* ── Right TOC Sidebar (Docusaurus-style) ── */
.toc-sidebar { width: 240px; min-width: 240px; position: sticky; top: var(--ifm-navbar-height);
               height: calc(100vh - var(--ifm-navbar-height)); overflow-y: auto;
               padding: 1rem 0.75rem 1rem 1rem; background: var(--ifm-background-color);
               scrollbar-width: thin; scrollbar-color: #686868 transparent;
               transition: width .25s ease, min-width .25s ease, padding .25s ease, opacity .25s ease; }
.toc-sidebar.collapsed { width: 0; min-width: 0; padding: 0; opacity: 0; overflow: hidden; }
.toc-sidebar::-webkit-scrollbar { width: 4px; }
.toc-sidebar::-webkit-scrollbar-thumb { background: #686868; border-radius: 4px; }
.toc-sidebar__title { display: none; }
.toc-sidebar__list { list-style: none; padding: 0; margin: 0;
                     border-left: 1px solid var(--ifm-color-emphasis-200); }
.toc-sidebar__item { margin: 0; }
.toc-sidebar__link { display: block; padding: 0.3rem 0 0.3rem 0.75rem; font-size: 0.8rem;
                     color: var(--ifm-color-emphasis-400); text-decoration: none;
                     transition: color .15s; line-height: 1.4;
                     border-left: 2px solid transparent; margin-left: -1px; }
.toc-sidebar__link:hover { color: var(--ifm-color-content); text-decoration: none; }
.toc-sidebar__link--active { color: var(--ifm-color-primary) !important;
                             border-left-color: var(--ifm-color-primary); }
.toc-sidebar__link--h3 { padding-left: 1.5rem; font-size: 0.75rem; }

/* ── Sidebar toggle button ── */
.sidebar-toggle { display: flex; align-items: center; justify-content: center; width: 32px; height: 32px;
                  border-radius: 50%; border: 1px solid var(--ifm-color-emphasis-200);
                  background: var(--ifm-background-color); color: var(--ifm-color-emphasis-400);
                  cursor: pointer; font-size: 16px; transition: all .2s; flex-shrink: 0; margin-right: 0.25rem; }
.sidebar-toggle:hover { border-color: var(--ifm-color-primary); color: var(--ifm-color-primary);
                        background: var(--ifm-hover-overlay); }
.sidebar-toggle svg { width: 18px; height: 18px; }
.sidebar.collapsed { width: 0; min-width: 0; padding: 0; overflow: hidden; border-right: none;
                     transition: width .25s ease, min-width .25s ease, padding .25s ease; }
.sidebar { transition: width .25s ease, min-width .25s ease, padding .25s ease; }

/* ── Theme toggle button ── */
.theme-toggle { display: flex; align-items: center; justify-content: center; width: 32px; height: 32px;
                border-radius: 50%; border: 1px solid var(--ifm-color-emphasis-200);
                background: var(--ifm-background-color); color: var(--ifm-color-emphasis-400);
                cursor: pointer; font-size: 16px; transition: all .2s; flex-shrink: 0; }
.theme-toggle:hover { border-color: var(--ifm-color-primary); color: var(--ifm-color-primary);
                      background: var(--ifm-hover-overlay); }
.theme-toggle svg { width: 18px; height: 18px; }

/* ── Light-theme specific tweaks ── */
[data-theme="light"] .sidebar { background: #f6f8fa; }
[data-theme="light"] .toc-sidebar { background: #fff; }
[data-theme="light"] table th { background: #f6f8fa; }
[data-theme="light"] tr:nth-child(even) td { background: #f9fafb; }
[data-theme="light"] tr:hover td { background: #eff6ff !important; }
[data-theme="light"] .kpi-card { background: #fff; border-color: #e2e8f0;
                                 box-shadow: 0 1px 3px rgba(0,0,0,.06); }
[data-theme="light"] .landing-capability-card { background: #fff; border-color: #e2e8f0;
                                                box-shadow: 0 1px 3px rgba(0,0,0,.06); }
[data-theme="light"] .score-breakdown { background: #f8fafc; border-color: #e2e8f0; }
[data-theme="light"] .badge-pass { background: rgba(22,163,74,.1); color: #16a34a; }
[data-theme="light"] .badge-fail { background: rgba(220,38,38,.1); color: #dc2626; }
[data-theme="light"] .badge-warn { background: rgba(202,138,4,.1); color: #ca8a04; }
[data-theme="light"] .badge-na { background: rgba(100,116,139,.1); color: #64748b; }
[data-theme="light"] .badge-critical { background: rgba(220,38,38,.1); color: #dc2626; }
[data-theme="light"] .badge-high { background: rgba(234,88,12,.1); color: #ea580c; }
[data-theme="light"] .badge-medium { background: rgba(202,138,4,.1); color: #ca8a04; }
[data-theme="light"] .badge-low { background: rgba(37,99,235,.1); color: #2563eb; }
[data-theme="light"] .pagination-nav__link { background: #fff; border-color: #e2e8f0;
                                             box-shadow: 0 1px 3px rgba(0,0,0,.04); }
[data-theme="light"] .breadcrumb__current { background: #ebedf0; color: #1c1e21; }
[data-theme="light"] .navbar__search { background: #f6f8fa; border-color: #e2e8f0; }
[data-theme="light"] .toc-toggle { background: #fff; border-color: #e2e8f0;
                                   box-shadow: 0 2px 6px rgba(0,0,0,.08); }

/* ── TOC toggle button ── */
.toc-toggle { position: fixed; right: 12px; bottom: 20px; z-index: 150; width: 36px; height: 36px;
              border-radius: 50%; border: 1px solid var(--ifm-color-emphasis-200);
              background: var(--ifm-background-surface-color); color: var(--ifm-color-emphasis-400);
              cursor: pointer; display: flex; align-items: center; justify-content: center;
              font-size: 14px; transition: all .2s; box-shadow: 0 2px 8px rgba(0,0,0,.3); }
.toc-toggle:hover { border-color: var(--ifm-color-primary); color: var(--ifm-color-primary);
                    background: var(--ifm-hover-overlay); }
.toc-toggle[title]:hover::after { content: attr(title); position: absolute; right: calc(100% + 8px);
  top: 50%; transform: translateY(-50%); white-space: nowrap; font-size: 0.7rem;
  background: var(--ifm-background-surface-color); border: 1px solid var(--ifm-color-emphasis-200);
  border-radius: var(--ifm-border-radius); padding: 4px 8px; color: var(--ifm-color-emphasis-600);
  box-shadow: 0 2px 6px rgba(0,0,0,.2); }

/* ── Breadcrumb (Docusaurus-style) ── */
.breadcrumb { display: flex; align-items: center; gap: 0.5rem; font-size: 0.85rem;
              color: var(--ifm-color-emphasis-400); margin-bottom: 1.25rem; flex-wrap: wrap; }
.breadcrumb__home { color: var(--ifm-color-emphasis-400); text-decoration: none; display: flex;
                    align-items: center; transition: color .15s; }
.breadcrumb__home:hover { color: var(--ifm-color-primary-light); }
.breadcrumb__home svg { width: 16px; height: 16px; }
.breadcrumb__item { color: var(--ifm-color-emphasis-400); text-decoration: none; transition: color .15s; }
.breadcrumb__item:hover { color: var(--ifm-color-primary-light); text-decoration: underline; }
.breadcrumb__sep { color: var(--ifm-color-emphasis-300); font-size: 0.7rem; }
.breadcrumb__current { color: var(--ifm-color-content); background: var(--ifm-color-emphasis-200);
                       padding: 0.15rem 0.6rem; border-radius: var(--ifm-border-radius);
                       font-size: 0.8rem; }

/* ── Prev/Next navigation ── */
.pagination-nav { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 2rem;
                  padding-top: 1.5rem; border-top: 1px solid var(--ifm-color-emphasis-200); }
.pagination-nav__link { display: block; padding: 1rem 1.25rem; border: 1px solid var(--ifm-color-emphasis-200);
                        border-radius: var(--ifm-card-border-radius); text-decoration: none;
                        transition: border-color .2s, background .2s; background: var(--ifm-background-surface-color);
                        cursor: pointer; }
.pagination-nav__link:hover { border-color: var(--ifm-color-primary); background: var(--ifm-hover-overlay);
                              text-decoration: none; }
.pagination-nav__link--next { text-align: right; grid-column: 2; }
.pagination-nav__link--prev { grid-column: 1; }
.pagination-nav__sublabel { font-size: 0.7rem; color: var(--ifm-color-emphasis-400);
                            text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 0.25rem;
                            display: block; }
.pagination-nav__label { font-size: 0.9rem; font-weight: var(--ifm-font-weight-bold);
                         color: var(--ifm-color-primary-light); }

/* ── Page Header (SchemaX docs-style) ── */
.landing-hero { margin: 0 0 1.5rem; padding: 0; text-align: left; }
.landing-hero__title { font-size: 2rem; font-weight: var(--ifm-font-weight-bold);
                       margin-bottom: 0.5rem; color: var(--ifm-color-emphasis-900);
                       line-height: 1.2; }
.landing-hero__tagline { color: var(--ifm-color-content-secondary); font-size: 1rem;
                         line-height: 1.5; margin-bottom: 0.5rem; }
.landing-hero__sub { color: var(--ifm-color-emphasis-400); font-size: 0.8rem;
                     font-family: var(--ifm-font-family-monospace); margin-bottom: 0.25rem; }
.landing-hero__links { display: flex; flex-wrap: wrap; gap: 0.75rem; justify-content: center;
                       margin-top: 1.5rem; }
.landing-hero__link { border-radius: var(--ifm-border-radius); display: inline-block;
                      font-weight: var(--ifm-font-weight-semibold); padding: 0.5rem 1rem;
                      transition: opacity .2s; text-decoration: none; font-size: 0.9rem; }
.landing-hero__link--primary { background: var(--ifm-color-primary); color: #fff; }
.landing-hero__link--primary:hover { opacity: 0.85; color: #fff; text-decoration: none; }
.landing-hero__link--secondary { border: 1px solid var(--ifm-color-primary);
                                 color: var(--ifm-color-primary-light); }
.landing-hero__link--secondary:hover { background: rgba(255,54,33,.08); text-decoration: none; }

/* ── KPI Strip ── */
.kpi-strip { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
             gap: 0.75rem; margin: 0 0 1.5rem; }
.kpi-card { background: var(--ifm-background-surface-color); border: 1px solid var(--ifm-color-emphasis-200);
            border-radius: var(--ifm-card-border-radius); padding: 1rem; text-align: center;
            box-shadow: 0 1px 2px 0 rgba(0,0,0,.1); transition: transform .2s, border-color .2s; }
.kpi-card:hover { transform: translateY(-2px); border-color: var(--ifm-color-primary); }
.kpi-card .kpi-val { font-size: clamp(1.25rem, 3vw, 1.75rem); font-weight: 800; margin: 0.25rem 0; }
.kpi-card .kpi-label { font-size: 0.65rem; color: var(--ifm-color-emphasis-400); text-transform: uppercase;
                       letter-spacing: 0.06em; }
.kpi-card .kpi-sub { font-size: 0.65rem; color: var(--ifm-color-emphasis-300); margin-top: 0.25rem; }

/* ── Capability Cards (SchemaX-style grid) ── */
.landing-capabilities { border-top: 1px solid var(--ifm-color-emphasis-200); padding: 2rem 0 1.5rem; }
.landing-capabilities__title { font-size: 1.75rem; margin-bottom: 2rem; text-align: center; }
.landing-capabilities__grid { display: grid; gap: 1rem;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); margin: 0; }
.landing-capability-card { background: var(--ifm-background-surface-color);
  border: 1px solid var(--ifm-color-emphasis-200); border-radius: var(--ifm-card-border-radius);
  padding: 1.25rem; transition: border-color .2s, transform .2s; }
.landing-capability-card:hover { border-color: var(--ifm-color-primary); transform: translateY(-2px); }
.landing-capability-card__title { font-size: 1rem; font-weight: 600; margin-bottom: 0.35rem;
                                  color: var(--ifm-color-emphasis-900); }
.landing-capability-card__value { font-size: 1.75rem; font-weight: 800; margin-bottom: 0.25rem; }
.landing-capability-card__desc { color: var(--ifm-color-content-secondary); font-size: 0.9rem;
                                 line-height: 1.45; margin: 0; }

/* ── Content Card ── */
.card { background: transparent; border: none; border-radius: 0; padding: 0;
        margin-bottom: 1.5rem; overflow: visible; }
.card h2 { font-size: 1.5rem; font-weight: var(--ifm-font-weight-bold); margin-bottom: 1rem;
           color: var(--ifm-color-emphasis-900); padding-bottom: 0.5rem;
           border-bottom: 1px solid var(--ifm-color-emphasis-200); }
.card h3 { font-size: 1rem; font-weight: 600; margin: 1.5rem 0 0.75rem;
           color: var(--ifm-color-content); }

/* ── Search ── */
.search-wrap { position: relative; margin-bottom: 1.5rem; }
.search-wrap input { width: 100%; padding: 0.6rem 1rem 0.6rem 2.5rem; border: 1px solid var(--ifm-color-emphasis-200);
  border-radius: var(--ifm-border-radius); font-size: 0.9rem; outline: none;
  background: rgba(255,255,255,.07); color: var(--ifm-color-content); transition: all .2s;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='none' stroke='%238d949e' stroke-width='2' stroke-linecap='round'%3E%3Ccircle cx='7' cy='7' r='5'/%3E%3Cline x1='11' y1='11' x2='15' y2='15'/%3E%3C/svg%3E");
  background-position: 0.75rem center; background-repeat: no-repeat; }
.search-wrap input:focus { border-color: var(--ifm-color-primary);
  box-shadow: 0 0 0 3px rgba(255,54,33,.15); }
.search-wrap input::placeholder { color: var(--ifm-color-emphasis-400); }
.search-count { position: absolute; right: 0.75rem; top: 50%; transform: translateY(-50%);
  font-size: 0.75rem; color: var(--ifm-color-emphasis-400); }

/* ── Tab Panels ── */
.tab-panel { display: none; }
.tab-panel.visible { display: block; }

/* ── Tables ── */
table { width: 100%; border-collapse: collapse; font-size: 0.8125rem; }
th { text-align: left; padding: 0.6rem 0.75rem; background: var(--ifm-background-surface-color);
     font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em;
     color: var(--ifm-color-emphasis-400); border-bottom: 2px solid var(--ifm-color-emphasis-200);
     position: sticky; top: 0; z-index: 1; white-space: nowrap; }
td { padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--ifm-color-emphasis-200);
     vertical-align: top; color: var(--ifm-color-content); }
tr:nth-child(even) td { background: rgba(255,255,255,.02); }
tr:hover td { background: var(--ifm-hover-overlay) !important; }

/* ── Status / Severity Badges ── */
.badge { padding: 2px 10px; border-radius: 9999px; font-size: 0.7rem; font-weight: 600;
         display: inline-block; }
.badge-pass { background: rgba(0,164,0,.15); color: #00a400; }
.badge-fail { background: rgba(250,56,62,.15); color: #fa383e; }
.badge-warn { background: rgba(255,186,0,.15); color: #ffba00; }
.badge-na { background: rgba(141,148,158,.15); color: #8d949e; }
.badge-critical { background: rgba(250,56,62,.15); color: #fa383e; }
.badge-high { background: rgba(249,115,22,.15); color: #f97316; }
.badge-medium { background: rgba(255,186,0,.15); color: #ffba00; }
.badge-low { background: rgba(84,199,236,.15); color: #54c7ec; }

/* ── Tooltips ── */
.tip { position: relative; cursor: help; }
.tip .tip-text { visibility: hidden; opacity: 0; position: absolute; bottom: calc(100% + 8px);
  left: 50%; transform: translateX(-50%); width: 220px; padding: 0.5rem 0.75rem;
  background: var(--ifm-background-surface-color); color: var(--ifm-color-content);
  font-size: 0.75rem; font-weight: 400; border-radius: 0.5rem; text-transform: none; z-index: 100;
  transition: opacity .15s; pointer-events: none; line-height: 1.4;
  box-shadow: 0 4px 12px rgba(0,0,0,.4); border: 1px solid var(--ifm-color-emphasis-200); }
.tip .tip-text::after { content: ''; position: absolute; top: 100%; left: 50%;
  margin-left: -5px; border: 5px solid transparent; border-top-color: var(--ifm-background-surface-color); }
.tip:hover .tip-text { visibility: visible; opacity: 1; }

/* ── Sortable ── */
th.sortable { cursor: pointer; user-select: none; }
th.sortable:hover { background: var(--ifm-color-emphasis-200); }
th.sortable::after { content: ' \\2195'; color: var(--ifm-color-emphasis-400); font-size: 0.625rem; }
th.sort-asc::after { content: ' \\2191'; color: var(--ifm-color-primary); }
th.sort-desc::after { content: ' \\2193'; color: var(--ifm-color-primary); }

/* ── Pagination ── */
.pager { display: flex; align-items: center; justify-content: center; gap: 4px;
         margin-top: 0.75rem; padding: 0.5rem 0; flex-wrap: wrap; }
.pager button { padding: 0.375rem 0.75rem; border: 1px solid var(--ifm-color-emphasis-200);
  border-radius: var(--ifm-border-radius); background: var(--ifm-background-surface-color);
  color: var(--ifm-color-emphasis-600); font-size: 0.75rem; cursor: pointer;
  transition: all .15s; min-width: 2.25rem; }
.pager button:hover { background: var(--ifm-hover-overlay); color: var(--ifm-color-content); }
.pager button.active { background: var(--ifm-color-primary); color: #fff;
  border-color: var(--ifm-color-primary); }
.pager button:disabled { opacity: .4; cursor: default; }
.pager .page-info { font-size: 0.75rem; color: var(--ifm-color-emphasis-400); margin: 0 0.5rem; }

/* ── Score breakdown ── */
.score-breakdown { background: var(--ifm-background-surface-color);
  border: 1px solid var(--ifm-color-emphasis-200); border-radius: var(--ifm-card-border-radius);
  padding: 1.25rem 1.5rem; margin-bottom: 1.5rem; }

/* ── Progress bars ── */
.progress-bar { height: 8px; background: var(--ifm-color-emphasis-200); border-radius: 4px; overflow: hidden; }
.progress-fill { height: 100%; border-radius: 4px; }

/* ── CTA Section ── */
.landing-cta { border-top: 1px solid var(--ifm-color-emphasis-200); padding: 2.5rem 1rem; text-align: center; }
.landing-cta__title { font-size: 1.5rem; margin-bottom: 0.5rem; }
.landing-cta__text { color: var(--ifm-color-content-secondary); margin-bottom: 1.25rem; }

/* ── Footer (Docusaurus dark) ── */
.footer { background-color: var(--ifm-footer-bg); color: var(--ifm-footer-link-color);
          padding: 2rem; text-align: center; font-size: 0.8rem; }
.footer__title { color: var(--ifm-footer-title-color); font-weight: var(--ifm-font-weight-bold);
                 margin-bottom: 0.5rem; }
.footer__links { display: flex; flex-wrap: wrap; gap: 1.5rem; justify-content: center;
                 margin-bottom: 1rem; }
.footer__link { color: var(--ifm-footer-link-color); text-decoration: none; font-size: 0.8rem; }
.footer__link:hover { color: #fff; text-decoration: underline; }
.footer__copyright { color: var(--ifm-color-emphasis-400); font-size: 0.7rem; margin-top: 1rem; }

/* ── Responsive ── */
@media (max-width: 1200px) {
  .toc-sidebar { display: none; }
  .toc-toggle { display: none; }
}
@media (max-width: 996px) {
  .sidebar { display: none; }
  .main { padding: 1rem; }
  .landing-hero__title { font-size: 1.75rem; }
  .pagination-nav { grid-template-columns: 1fr; }
  .pagination-nav__link--next { grid-column: 1; text-align: left; }
}
@media (max-width: 768px) {
  .kpi-strip { grid-template-columns: repeat(2, 1fr); gap: 0.5rem; }
  .landing-capabilities__grid { grid-template-columns: 1fr; }
  .navbar__version { display: none; }
}

/* ── Print ── */
@media print {
  .navbar, .sidebar, .toc-sidebar, .toc-toggle, .search-wrap, .pager, .footer, .pagination-nav, .breadcrumb { display: none !important; }
  .layout { display: block; }
  .main { padding: 0; }
  .tab-panel { display: block !important; page-break-inside: avoid; margin-bottom: 1.5rem; }
  body { background: #fff; color: #1c1e21; }
  .card { background: #fff; color: #1c1e21; }
  .card h2 { border-color: #dadde1; }
  .kpi-card, .landing-capability-card { background: #fff; border-color: #dadde1;
    color: #1c1e21; box-shadow: none; }
  td, th { color: #1c1e21; border-color: #dadde1; }
  th { background: #f5f6f7; }
}
"""

# ---------------------------------------------------------------------------
# JS
# ---------------------------------------------------------------------------

_MODERN_JS = """\
var PAGE_SIZE=25;
var pageState={};

// ── Theme toggle ──
function toggleTheme(){
  var html=document.documentElement;
  var current=html.getAttribute('data-theme');
  var next=(current==='dark')?'light':'dark';
  html.setAttribute('data-theme',next);
  try{localStorage.setItem('satscanner-theme',next);}catch(e){}
  updateThemeIcon(next);
}
function updateThemeIcon(theme){
  var btn=document.getElementById('theme-toggle-btn');
  if(!btn)return;
  if(theme==='light'){
    btn.innerHTML='<svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z\"/></svg>';
    btn.title='Switch to dark mode';
  } else {
    btn.innerHTML='<svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"12\" cy=\"12\" r=\"5\"/><line x1=\"12\" y1=\"1\" x2=\"12\" y2=\"3\"/><line x1=\"12\" y1=\"21\" x2=\"12\" y2=\"23\"/><line x1=\"4.22\" y1=\"4.22\" x2=\"5.64\" y2=\"5.64\"/><line x1=\"18.36\" y1=\"18.36\" x2=\"19.78\" y2=\"19.78\"/><line x1=\"1\" y1=\"12\" x2=\"3\" y2=\"12\"/><line x1=\"21\" y1=\"12\" x2=\"23\" y2=\"12\"/><line x1=\"4.22\" y1=\"19.78\" x2=\"5.64\" y2=\"18.36\"/><line x1=\"18.36\" y1=\"5.64\" x2=\"19.78\" y2=\"4.22\"/></svg>';
    btn.title='Switch to light mode';
  }
}
// Apply saved theme preference
(function(){
  try{
    var saved=localStorage.getItem('satscanner-theme');
    if(saved){document.documentElement.setAttribute('data-theme',saved);updateThemeIcon(saved);}
  }catch(e){}
})();

// ── Sidebar toggle ──
function toggleSidebar(){
  var sidebar=document.querySelector('.sidebar');
  var btn=document.getElementById('sidebar-toggle-btn');
  if(!sidebar||!btn)return;
  if(sidebar.classList.contains('collapsed')){
    sidebar.classList.remove('collapsed');
    btn.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>';
    btn.title='Hide sidebar';
    try{localStorage.removeItem('satscanner-sidebar-hidden');}catch(e){}
  } else {
    sidebar.classList.add('collapsed');
    btn.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>';
    btn.title='Show sidebar';
    try{localStorage.setItem('satscanner-sidebar-hidden','1');}catch(e){}
  }
}
// Restore sidebar state
(function(){
  try{
    if(localStorage.getItem('satscanner-sidebar-hidden')==='1'){
      var sidebar=document.querySelector('.sidebar');
      if(sidebar)sidebar.classList.add('collapsed');
    }
  }catch(e){}
})();

function getVisibleRows(panel){
  var rows=[];
  var all=panel.querySelectorAll('tr');
  for(var i=0;i<all.length;i++){
    if(all[i].querySelector('th'))continue;
    if(all[i].getAttribute('data-filtered')!=='hidden')rows.push(all[i]);
  }
  return rows;
}

function paginate(panelId,page){
  var panel=document.getElementById('panel-'+panelId);
  if(!panel)return;
  var rows=getVisibleRows(panel);
  var totalPages=Math.max(1,Math.ceil(rows.length/PAGE_SIZE));
  if(page<1)page=1;if(page>totalPages)page=totalPages;
  pageState[panelId]=page;
  for(var i=0;i<rows.length;i++){
    var p=Math.floor(i/PAGE_SIZE)+1;
    rows[i].style.display=(p===page)?'':'none';
  }
  var pager=panel.querySelector('.pager');
  if(!pager)return;
  if(rows.length<=PAGE_SIZE){pager.style.display='none';return;}
  pager.style.display='flex';
  var html='<button onclick="paginate(\\''+panelId+'\\','+(page-1)+')"'+(page<=1?' disabled':'')+'>Prev</button>';
  var start=Math.max(1,page-2),end=Math.min(totalPages,page+2);
  if(start>1)html+='<button onclick="paginate(\\''+panelId+'\\',1)">1</button>';
  if(start>2)html+='<span class="page-info">...</span>';
  for(var p=start;p<=end;p++){
    html+='<button class="'+(p===page?'active':'')+'" onclick="paginate(\\''+panelId+'\\','+p+')">'+p+'</button>';
  }
  if(end<totalPages-1)html+='<span class="page-info">...</span>';
  if(end<totalPages)html+='<button onclick="paginate(\\''+panelId+'\\','+totalPages+')">'+totalPages+'</button>';
  html+='<button onclick="paginate(\\''+panelId+'\\','+(page+1)+')"'+(page>=totalPages?' disabled':'')+'>Next</button>';
  html+='<span class="page-info">'+rows.length+' rows</span>';
  pager.innerHTML=html;
}

function doSearch(q){
  q=q.toLowerCase().trim();
  var total=0;
  document.querySelectorAll('.tab-panel').forEach(function(panel){
    var tc=0;
    var rows=panel.querySelectorAll('tr');
    for(var i=0;i<rows.length;i++){
      if(rows[i].querySelector('th'))continue;
      var t=rows[i].textContent.toLowerCase();
      if(!q||t.indexOf(q)>=0){
        rows[i].removeAttribute('data-filtered');
        if(q)tc++;
      }else{
        rows[i].setAttribute('data-filtered','hidden');
        rows[i].style.display='none';
      }
    }
    total+=tc;
    var tid=panel.id.replace('panel-','');
    var btn=document.querySelector('.menu__link[data-tab="'+tid+'"]');
    if(btn){
      var b=btn.querySelector('.search-badge');
      if(!b){b=document.createElement('span');b.className='search-badge menu__badge';btn.appendChild(b);}
      b.textContent=(q&&tc)?tc:'';
      b.style.display=(q&&tc)?'inline-block':'none';
    }
    paginate(tid,1);
  });
  var badge=document.getElementById('searchCount');
  if(badge)badge.textContent=q?total+' match'+(total!==1?'es':''):'';
}

// TOC sidebar state
var tocManuallyHidden=false;
var tocHasContent=false;

// Build right-side TOC from h2/h3 in active panel
function buildTOC(panel){
  var toc=document.getElementById('toc-list');
  var tocSidebar=document.querySelector('.toc-sidebar');
  var tocBtn=document.getElementById('toc-toggle-btn');
  if(!toc||!tocSidebar)return;
  toc.innerHTML='';
  if(!panel){tocHasContent=false;autoCollapseTOC();return;}
  var headings=panel.querySelectorAll('h2,h3');
  if(headings.length<2){
    tocHasContent=false;
    autoCollapseTOC();
    return;
  }
  tocHasContent=true;
  headings.forEach(function(h,i){
    if(!h.id)h.id='toc-'+panel.id+'-'+i;
    var li=document.createElement('li');
    li.className='toc-sidebar__item';
    var a=document.createElement('a');
    a.className='toc-sidebar__link'+(h.tagName==='H3'?' toc-sidebar__link--h3':'');
    a.href='#'+h.id;
    a.textContent=h.textContent;
    a.addEventListener('click',function(e){
      e.preventDefault();
      h.scrollIntoView({behavior:'smooth',block:'start'});
    });
    li.appendChild(a);
    toc.appendChild(li);
  });
  autoCollapseTOC();
  // Scroll-spy: highlight TOC item nearest to top
  var mainEl=document.querySelector('.main');
  if(mainEl&&!mainEl._tocSpy){
    mainEl._tocSpy=true;
    mainEl.addEventListener('scroll',function(){
      if(!tocHasContent)return;
      var links=toc.querySelectorAll('.toc-sidebar__link');
      var scrollTop=mainEl.scrollTop;
      var active=null;
      links.forEach(function(a){
        var target=document.getElementById(a.href.split('#')[1]);
        if(target&&target.offsetTop<=scrollTop+100)active=a;
      });
      links.forEach(function(a){a.classList.remove('toc-sidebar__link--active')});
      if(active)active.classList.add('toc-sidebar__link--active');
    });
  }
}

function autoCollapseTOC(){
  var tocSidebar=document.querySelector('.toc-sidebar');
  var tocBtn=document.getElementById('toc-toggle-btn');
  if(!tocSidebar)return;
  if(!tocHasContent){
    tocSidebar.classList.add('collapsed');
    if(tocBtn){tocBtn.style.display='none';}
  } else if(!tocManuallyHidden){
    tocSidebar.classList.remove('collapsed');
    if(tocBtn){tocBtn.style.display='';tocBtn.innerHTML='&#10005;';tocBtn.title='Hide table of contents';}
  } else {
    if(tocBtn){tocBtn.style.display='';tocBtn.innerHTML='&#9776;';tocBtn.title='Show table of contents';}
  }
}

function toggleTOC(){
  var tocSidebar=document.querySelector('.toc-sidebar');
  var tocBtn=document.getElementById('toc-toggle-btn');
  if(!tocSidebar||!tocHasContent)return;
  if(tocSidebar.classList.contains('collapsed')){
    tocSidebar.classList.remove('collapsed');
    tocManuallyHidden=false;
    if(tocBtn){tocBtn.innerHTML='&#10005;';tocBtn.title='Hide table of contents';}
  }else{
    tocSidebar.classList.add('collapsed');
    tocManuallyHidden=true;
    if(tocBtn){tocBtn.innerHTML='&#9776;';tocBtn.title='Show table of contents';}
  }
}

// Update breadcrumb text
function updateBreadcrumb(label){
  var bc=document.getElementById('breadcrumb-current');
  if(bc)bc.textContent=label||'';
}

// Navigate to tab by id
function switchTab(tid){
  var btn=document.querySelector('.menu__link[data-tab="'+tid+'"]');
  if(btn)btn.click();
}

// Tab click handler
var tabButtons=document.querySelectorAll('.menu__link[data-tab]');
tabButtons.forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.menu__link').forEach(function(b){b.classList.remove('menu__link--active')});
    document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('visible')});
    btn.classList.add('menu__link--active');
    var tid=btn.getAttribute('data-tab');
    var panel=document.getElementById('panel-'+tid);
    if(panel)panel.classList.add('visible');
    // Update breadcrumb
    var label=btn.querySelector('span');
    updateBreadcrumb(label?label.textContent:'');
    // Build TOC
    buildTOC(panel);
    // Search or paginate
    var si=document.getElementById('searchInput');
    if(si&&si.value)doSearch(si.value);
    else paginate(tid,pageState[tid]||1);
    // Scroll to top
    var mainEl=document.querySelector('.main');
    if(mainEl)mainEl.scrollTop=0;
  });
});

// Init pagination + TOC for first visible panel
document.querySelectorAll('.tab-panel').forEach(function(panel){
  var tid=panel.id.replace('panel-','');
  paginate(tid,1);
});
var firstVisible=document.querySelector('.tab-panel.visible');
if(firstVisible)buildTOC(firstVisible);

// Init breadcrumb from first active tab
var firstActive=document.querySelector('.menu__link--active[data-tab]');
if(firstActive){
  var span=firstActive.querySelector('span');
  updateBreadcrumb(span?span.textContent:'');
}

// Sortable columns
document.querySelectorAll('th.sortable').forEach(function(th){
  th.addEventListener('click',function(){
    var table=th.closest('table');
    if(!table)return;
    var tbody=table.querySelector('tbody');
    if(!tbody)return;
    var idx=Array.prototype.indexOf.call(th.parentNode.children,th);
    var asc=!th.classList.contains('sort-asc');
    th.parentNode.querySelectorAll('th').forEach(function(h){h.classList.remove('sort-asc','sort-desc')});
    th.classList.add(asc?'sort-asc':'sort-desc');
    var rows=Array.from(tbody.querySelectorAll('tr'));
    rows.sort(function(a,b){
      var at=(a.children[idx]||{}).textContent||'';
      var bt=(b.children[idx]||{}).textContent||'';
      var an=parseFloat(at.replace(/[^0-9.\\-]/g,''));
      var bn=parseFloat(bt.replace(/[^0-9.\\-]/g,''));
      if(!isNaN(an)&&!isNaN(bn))return asc?an-bn:bn-an;
      return asc?at.localeCompare(bt):bt.localeCompare(at);
    });
    rows.forEach(function(r){tbody.appendChild(r)});
    var panel=table.closest('.tab-panel');
    if(panel)paginate(panel.id.replace('panel-',''),1);
  });
});
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_definitions_html(show_effort: bool = False) -> str:
    """Build the Definitions tab HTML content using CSS variables."""
    html = """<h3>Finding Statuses</h3>
<table class="sortable-table"><thead><tr><th>Status</th><th>Label</th><th>Definition</th></tr></thead><tbody>
<tr><td><span class="badge badge-pass">PASS</span></td><td>Compliant</td><td>The check confirmed the security control is in place. No action needed.</td></tr>
<tr><td><span class="badge badge-fail">FAIL</span></td><td>Action Required</td><td>Confirmed security gap that must be fixed.</td></tr>
<tr><td><span class="badge badge-warn">WARN</span></td><td>Review Needed</td><td>Borderline result that needs manual investigation.</td></tr>
<tr><td><span class="badge badge-na">N/A</span></td><td>Skipped</td><td>Feature not in use on this workspace. Excluded from score.</td></tr>
<tr><td><span class="badge badge-critical">API ERROR</span></td><td>Could Not Evaluate</td><td>API failure prevented this check. Excluded from score.</td></tr>
</tbody></table>
<h3>Severity Levels</h3>
<table class="sortable-table"><thead><tr><th>Severity</th><th style="text-align:center">Weight</th><th>Definition</th></tr></thead><tbody>
<tr><td><span class="badge badge-critical">CRITICAL</span></td><td style="text-align:center;font-weight:700">10</td><td>Immediate security risk. Must be remediated immediately.</td></tr>
<tr><td><span class="badge badge-high">HIGH</span></td><td style="text-align:center;font-weight:700">7</td><td>Significant weakness. Remediate within days.</td></tr>
<tr><td><span class="badge badge-medium">MEDIUM</span></td><td style="text-align:center;font-weight:700">4</td><td>Moderate risk. Remediate within current sprint.</td></tr>
<tr><td><span class="badge badge-low">LOW</span></td><td style="text-align:center;font-weight:700">2</td><td>Minor improvement. Plan for upcoming cycles.</td></tr>
</tbody></table>
<h3>Grade Definitions</h3>
<table class="sortable-table"><thead><tr><th>Grade</th><th>Score Range</th><th>Definition</th></tr></thead><tbody>
<tr><td><span class="badge badge-pass">Good</span></td><td>80 &ndash; 100</td><td>Strong security posture. Address remaining findings as maintenance.</td></tr>
<tr><td><span class="badge badge-warn">Needs Improvement</span></td><td>60 &ndash; 79</td><td>Gaps weaken security posture. Prioritize High/Critical findings.</td></tr>
<tr><td><span class="badge badge-fail">Critical</span></td><td>0 &ndash; 59</td><td>Significant risks present. Immediate remediation required.</td></tr>
</tbody></table>
<h3>Scoring Formula</h3>
<table class="sortable-table"><thead><tr><th>Component</th><th>Detail</th></tr></thead><tbody>
<tr><td>Weight per check</td><td>Critical=10, High=7, Medium=4, Low=2</td></tr>
<tr><td>FAIL penalty</td><td>Full weight</td></tr>
<tr><td>WARN penalty</td><td>Half weight</td></tr>
<tr><td>PASS penalty</td><td>0</td></tr>
<tr><td>Excluded</td><td>NOT_APPLICABLE and API Error findings</td></tr>
<tr><td>Formula</td><td>Score = (1 &minus; penalty_sum / total_weights) &times; 100</td></tr>
</tbody></table>
<h3>Category Definitions</h3>
<table class="sortable-table"><thead><tr><th>Category</th><th>Definition</th></tr></thead><tbody>
""" + "".join(f'<tr><td>{_esc(cat)}</td><td>{_esc(defn)}</td></tr>' for cat, defn in CATEGORY_DEFINITIONS) + """
</tbody></table>
<h3>HTTP Error Codes</h3>
<table class="sortable-table"><thead><tr><th>Code</th><th>Meaning</th><th>What to do</th></tr></thead><tbody>
<tr><td>401</td><td>Unauthorized</td><td>Regenerate PAT token or re-login via Azure.</td></tr>
<tr><td>403</td><td>Permission Denied</td><td>Use a Workspace Admin PAT token.</td></tr>
<tr><td>404</td><td>Not Found</td><td>Feature not enabled or requires Premium tier.</td></tr>
<tr><td>400</td><td>Bad Request</td><td>Feature managed differently (e.g. Unity Catalog).</td></tr>
</tbody></table>"""
    if show_effort:
        html += """<h3>Remediation Effort Levels</h3>
<table class="sortable-table"><thead><tr><th>Level</th><th>Time Range</th><th>What&rsquo;s Included</th></tr></thead><tbody>
<tr><td><span class="badge badge-pass">Quick Fix</span></td><td>5&ndash;15 min</td><td>Single configuration toggle. No coordination needed.</td></tr>
<tr><td><span class="badge badge-warn">Moderate</span></td><td>1&ndash;4 hrs</td><td>Multi-step config, IaC updates, pipeline runs.</td></tr>
<tr><td><span class="badge badge-high">Significant</span></td><td>1&ndash;3 days</td><td>Architecture changes, cross-team coordination.</td></tr>
<tr><td><span class="badge badge-na">Project</span></td><td>1+ weeks</td><td>Major migration, org-wide policy rollout.</td></tr>
</tbody></table>"""
    return html


def _score_gauge_svg(score: int, size: int = 90) -> str:
    """SVG circular gauge for compliance score."""
    r = size // 2 - 5
    circ = 2 * 3.14159 * r
    pct = max(0, min(100, score)) / 100
    color = "var(--pass)" if score >= 80 else ("var(--warn)" if score >= 60 else "var(--fail)")
    grade = "Good" if score >= 80 else ("Needs Improvement" if score >= 60 else "Critical")
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}">'
        f'<circle cx="{size//2}" cy="{size//2}" r="{r}" fill="none" stroke="var(--ifm-color-emphasis-200)" stroke-width="6"/>'
        f'<circle cx="{size//2}" cy="{size//2}" r="{r}" fill="none" stroke="{color}" stroke-width="6"'
        f' stroke-dasharray="{circ * pct:.1f} {circ:.1f}"'
        f' stroke-linecap="round" transform="rotate(-90 {size//2} {size//2})"/>'
        f'<text x="{size//2}" y="{size//2 - 4}" text-anchor="middle" font-size="{size//4}"'
        f' font-weight="800" fill="{color}">{score}</text>'
        f'<text x="{size//2}" y="{size//2 + 12}" text-anchor="middle" font-size="{size//9}"'
        f' fill="var(--ifm-color-emphasis-400)">/ 100</text>'
        f'</svg>'
    )


_SEV_BADGE = {"critical": "badge-critical", "high": "badge-high", "medium": "badge-medium", "low": "badge-low"}
_STATUS_BADGE = {"PASS": "badge-pass", "FAIL": "badge-fail", "WARN": "badge-warn", "NOT_APPLICABLE": "badge-na"}

def _badge(text: str, css_class: str) -> str:
    return f'<span class="badge {css_class}">{_esc(str(text))}</span>'

def _sev_badge(severity: str) -> str:
    return _badge(severity.upper(), _SEV_BADGE.get(severity, "badge-na"))

def _status_badge(status: str) -> str:
    return _badge(status, _STATUS_BADGE.get(status, "badge-na"))


# ---------------------------------------------------------------------------
# Export functions (placeholders)
# ---------------------------------------------------------------------------

def export_html_modern(result: SATScanResult, output_dir: Path, include_api_response: bool = True,
                       summary_link: str = "", show_scan_items: bool = False, show_evidence: bool = False,
                       show_effort: bool = False, show_cost: bool = False) -> str:
    """Generate a modern SchemaX/Docusaurus-style single-workspace HTML report."""
    import json as _json_mod

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.fromisoformat(result.scanned_at.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S UTC")
    score = result.overall_score

    def _portal_label(link: str) -> str:
        if "portal.azure.com" in link:
            return "Azure Portal"
        if "accounts.azuredatabricks.net" in link:
            return "Account Console"
        return "Open"

    # ── Score breakdown data ──
    _SEV_W = {"critical": 10, "high": 7, "medium": 4, "low": 2}
    _scorable = [f for f in result.findings if not f.is_api_error]
    _applicable = [f for f in _scorable if f.status != "NOT_APPLICABLE"]
    _fail_checks = [f for f in _applicable if f.status == "FAIL"]
    _warn_checks = [f for f in _applicable if f.status == "WARN"]
    _pass_checks = [f for f in _applicable if f.status == "PASS"]
    _total_wt = sum(_SEV_W.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _applicable)
    _fail_pen = sum(_SEV_W.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _fail_checks)
    _warn_pen = sum(_SEV_W.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) * 0.5 for f in _warn_checks)
    _total_pen = _fail_pen + _warn_pen
    score_color = "var(--pass)" if score >= 80 else ("var(--warn)" if score >= 60 else "var(--fail)")
    grade = "Good" if score >= 80 else ("Needs Improvement" if score >= 60 else "Critical")

    # ── Summary tab content ──
    score_gauge = _score_gauge_svg(score, 130)

    # Score breakdown
    score_breakdown = f"""<div class="score-breakdown">
<h3>Score Breakdown</h3>
<table style="font-size:13px;border:none">
<tr><td style="border:none;padding:4px 12px 4px 0;color:var(--ifm-color-emphasis-400)">Scored checks</td>
<td style="border:none;padding:4px 0;font-weight:600">{len(_applicable)} <span style="color:var(--ifm-color-emphasis-300);font-weight:400">(excl. {result.not_applicable} N/A + {result.api_errors} API Error)</span></td></tr>
<tr><td style="border:none;padding:4px 12px 4px 0;color:var(--ifm-color-emphasis-400)">Total weight pool</td>
<td style="border:none;padding:4px 0;font-weight:600">{_total_wt:.0f} pts</td></tr>
<tr><td style="border:none;padding:4px 12px 4px 0;color:var(--fail)">FAIL penalty ({len(_fail_checks)} &times; full)</td>
<td style="border:none;padding:4px 0;font-weight:600;color:var(--fail)">&minus;{_fail_pen:.0f} pts</td></tr>
<tr><td style="border:none;padding:4px 12px 4px 0;color:var(--warn)">WARN penalty ({len(_warn_checks)} &times; half)</td>
<td style="border:none;padding:4px 0;font-weight:600;color:var(--warn)">&minus;{_warn_pen:.1f} pts</td></tr>
<tr><td style="border:none;padding:4px 12px 4px 0;color:var(--pass)">PASS ({len(_pass_checks)})</td>
<td style="border:none;padding:4px 0;font-weight:600;color:var(--pass)">0 pts</td></tr>
<tr style="border-top:1px solid var(--ifm-color-emphasis-200)">
<td style="border:none;padding:8px 12px 4px 0;font-weight:600">Formula</td>
<td style="border:none;padding:8px 0 4px;font-family:var(--ifm-font-family-monospace);font-size:12px">(1 &minus; {_total_pen:.1f} / {_total_wt:.0f}) &times; 100 = <strong style="color:{score_color};font-size:14px">{score}</strong></td></tr>
</table></div>"""

    # Category scores with progress bars
    cat_score_rows = ""
    for cat, cat_score in sorted(result.category_scores.items(), key=lambda x: x[1]):
        c = "var(--pass)" if cat_score >= 80 else ("var(--warn)" if cat_score >= 60 else "var(--fail)")
        g = "Good" if cat_score >= 80 else ("Needs Improvement" if cat_score >= 60 else "Critical")
        cat_score_rows += f'<tr><td>{_esc(cat)}</td><td style="font-weight:700;text-align:right;color:{c}">{cat_score}</td><td><div class="progress-bar"><div class="progress-fill" style="background:{c};width:{max(2, cat_score)}%"></div></div></td><td style="font-size:12px;color:{c}">{_esc(g)}</td></tr>'

    summary_content = f"""<div style="text-align:center;margin-bottom:24px">{score_gauge}</div>
{score_breakdown}
<table class="sortable-table"><thead><tr><th>Category</th><th class="sortable" style="text-align:right">Score</th><th style="width:200px">Progress</th><th>Grade</th></tr></thead>
<tbody>{cat_score_rows}</tbody></table>"""

    # ── All Findings tab ──
    all_findings_html = ""
    for f in result.findings:
        portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
        effort_td = f'<td style="font-size:12px;white-space:nowrap">{_esc(f.effort)}</td>' if show_effort else ''
        all_findings_html += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{_sev_badge(f.severity)}</td><td>{_status_badge(f.status)}</td>{effort_td}<td>{_esc(f.title)}</td><td style="font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(f.current_state)}</td><td>{portal}</td><td style="font-size:12px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(f.benefits) if f.benefits else ""}</td></tr>'
    effort_th = '<th class="sortable">Effort</th>' if show_effort else ''
    all_findings_content = f'<div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th><th class="sortable">Status</th>{effort_th}<th>Title</th><th>Current State</th><th>Portal</th><th>Why It Matters</th></tr></thead><tbody>{all_findings_html}</tbody></table><div class="pager"></div></div>'

    # ── Failed Checks tab ──
    failed = [f for f in result.findings if f.status == "FAIL"]
    if failed:
        failed_rows = ""
        for f in failed:
            ref = f'<a href="{_esc(f.reference_url)}" target="_blank" style="color:var(--info);font-size:12px">Docs</a>' if f.reference_url else ""
            portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
            effort_td = f'<td style="font-size:12px;white-space:nowrap">{_esc(f.effort)}</td>' if show_effort else ''
            secret_detail = ""
            _fc = 10 if show_effort else 9
            if f.details and "findings" in f.details:
                secret_detail = f'<tr><td colspan="{_fc}" style="padding:0 8px 12px">{_render_secret_details_html(f.details)}</td></tr>'
            failed_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{_sev_badge(f.severity)}</td>{effort_td}<td>{_esc(f.title)}</td><td style="font-size:12px">{_esc(f.current_state)}</td><td style="font-size:12px">{_esc(f.recommendation)}</td><td>{ref}</td><td>{portal}</td><td style="font-size:12px">{_esc(f.benefits) if f.benefits else ""}</td></tr>{secret_detail}'
        effort_th2 = '<th class="sortable">Effort</th>' if show_effort else ''
        failed_content = f'<div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th>{effort_th2}<th>Title</th><th>Current State</th><th>Recommendation</th><th>Ref</th><th>Portal</th><th>Why It Matters</th></tr></thead><tbody>{failed_rows}</tbody></table><div class="pager"></div></div>'
    else:
        failed_content = '<p style="color:var(--pass);font-weight:600">No failed checks. All checks passed or are warnings.</p>'

    # ── Warnings tab ──
    warns = [f for f in result.findings if f.status == "WARN" and not f.is_api_error]
    if warns:
        warn_rows = ""
        for f in warns:
            ref = f'<a href="{_esc(f.reference_url)}" target="_blank" style="color:var(--info);font-size:12px">Docs</a>' if f.reference_url else ""
            portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
            effort_td = f'<td style="font-size:12px;white-space:nowrap">{_esc(f.effort)}</td>' if show_effort else ''
            warn_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{_sev_badge(f.severity)}</td>{effort_td}<td>{_esc(f.title)}</td><td style="font-size:12px">{_esc(f.current_state)}</td><td style="font-size:12px">{_esc(f.recommendation)}</td><td>{ref}</td><td>{portal}</td><td style="font-size:12px">{_esc(f.benefits) if f.benefits else ""}</td></tr>'
        effort_th3 = '<th class="sortable">Effort</th>' if show_effort else ''
        warnings_content = f'<div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th>{effort_th3}<th>Title</th><th>Current State</th><th>Recommendation</th><th>Ref</th><th>Portal</th><th>Why It Matters</th></tr></thead><tbody>{warn_rows}</tbody></table><div class="pager"></div></div>'
    else:
        warnings_content = '<p style="color:var(--pass);font-weight:600">No warnings found.</p>'

    # ── API Errors tab ──
    api_errs = [f for f in result.findings if f.is_api_error]
    if api_errs:
        api_err_rows = ""
        for f in api_errs:
            http_code = f.details.get("http_status", "") if f.details else ""
            http_label = f"HTTP {http_code}" if http_code else "Exception"
            http_badge = _badge(http_label, "badge-critical")
            justification = f.details.get("justification", "") if f.details else ""
            just_html = f'<div style="color:var(--ifm-color-emphasis-400);font-size:11px;margin-top:4px;font-style:italic">{_esc(justification)}</div>' if justification else ""
            portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
            api_err_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{http_badge}</td><td>{_esc(f.title)}</td><td style="font-size:12px">{_esc(f.current_state)}{just_html}</td><td style="font-size:12px">{_esc(f.recommendation)}</td><td>{portal}</td><td style="font-size:12px">{_esc(f.benefits) if f.benefits else ""}</td></tr>'
        api_errors_content = f'<p style="color:var(--ifm-color-primary);font-size:13px;margin-bottom:16px;font-weight:600">{_pl(len(api_errs), "check")} could not be evaluated due to API failures. Excluded from score.</p><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th>HTTP Status</th><th>Title</th><th>Error Detail</th><th>Recommendation</th><th>Portal</th><th>Why It Matters</th></tr></thead><tbody>{api_err_rows}</tbody></table><div class="pager"></div></div>'
    else:
        api_errors_content = '<p style="color:var(--pass);font-weight:600">No API errors. All checks were evaluated successfully.</p>'

    # ── N/A tab ──
    na_findings = [f for f in result.findings if f.status == "NOT_APPLICABLE" and not f.is_api_error]
    if na_findings:
        na_rows = ""
        for f in na_findings:
            orig_sev = SAT_CHECKS.get(f.check_id, {}).get("severity", "low")
            ref = f'<a href="{_esc(f.reference_url)}" target="_blank" style="color:var(--info);font-size:12px">Docs</a>' if f.reference_url else ""
            portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
            na_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{_sev_badge(orig_sev)}</td><td>{_esc(f.title)}</td><td style="font-size:12px">{_esc(f.current_state)}</td><td style="font-size:12px">{_esc(f.recommendation)}</td><td>{ref}</td><td>{portal}</td></tr>'
        na_content = f'<p style="color:var(--ifm-color-emphasis-400);font-size:13px;margin-bottom:16px">These checks were skipped — feature not in use. Excluded from score.</p><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th><th>Title</th><th>Reason</th><th>Recommendation</th><th>Ref</th><th>Portal</th></tr></thead><tbody>{na_rows}</tbody></table><div class="pager"></div></div>'
    else:
        na_content = '<p style="color:var(--pass);font-weight:600">No skipped checks. All checks were evaluated.</p>'

    # ── Passed tab ──
    passed = [f for f in result.findings if f.status == "PASS"]
    if passed:
        pass_rows = ""
        for f in passed:
            orig_sev = SAT_CHECKS.get(f.check_id, {}).get("severity", "low")
            ref = f'<a href="{_esc(f.reference_url)}" target="_blank" style="color:var(--info);font-size:12px">Docs</a>' if f.reference_url else ""
            portal = f'<a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(f.portal_link)}&nbsp;&#8599;</a>' if f.portal_link else ""
            pass_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f.check_id)}</td><td style="font-size:12px">{_esc(f.category)}</td><td>{_sev_badge(orig_sev)}</td><td>{_esc(f.title)}</td><td style="font-size:12px">{_esc(f.current_state)}</td><td style="font-size:12px">{_esc(f.recommendation)}</td><td>{ref}</td><td>{portal}</td></tr>'
        passed_content = f'<p style="color:var(--pass);font-size:13px;margin-bottom:16px;font-weight:600">{len(passed)} check(s) passed successfully.</p><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th><th>Title</th><th>Current State</th><th>Recommendation</th><th>Ref</th><th>Portal</th></tr></thead><tbody>{pass_rows}</tbody></table><div class="pager"></div></div>'
    else:
        passed_content = '<p style="color:var(--warn);font-weight:600">No checks passed.</p>'

    # ── Per-category finding cards ──
    by_cat: dict[str, list[SATFinding]] = {}
    for f in result.findings:
        by_cat.setdefault(f.category, []).append(f)

    cat_content: dict[str, str] = {}
    for cat, findings in by_cat.items():
        cat_pass = sum(1 for f in findings if f.status == "PASS")
        cat_fail = sum(1 for f in findings if f.status == "FAIL")
        cat_warn = sum(1 for f in findings if f.status == "WARN")
        cat_na = sum(1 for f in findings if f.status == "NOT_APPLICABLE")
        cat_summary = f'<div style="display:flex;gap:16px;margin-bottom:16px;font-size:13px"><span style="color:var(--pass);font-weight:600">{_pl(cat_pass, "Passed", "Passed")}</span><span style="color:var(--fail);font-weight:600">{_pl(cat_fail, "Failed", "Failed")}</span><span style="color:var(--warn);font-weight:600">{_pl(cat_warn, "Warning")}</span><span style="color:var(--ifm-color-emphasis-400)">{cat_na} N/A</span></div>'
        items = ""
        for f in findings:
            rec_block = f'<div style="background:rgba(37,99,235,.08);border-left:4px solid var(--info);padding:10px 14px;border-radius:0 6px 6px 0;margin-top:8px"><strong style="color:var(--info)">Recommendation:</strong><br><span style="font-size:13px">{_esc(f.recommendation)}</span></div>' if f.status != "PASS" else ""
            ref_link = f'<p style="margin:8px 0 0;font-size:12px"><a href="{_esc(f.reference_url)}" style="color:var(--info)">Docs &#8599;</a></p>' if f.reference_url else ""
            portal_link = f'<p style="margin:4px 0 0;font-size:12px"><a href="{_esc(f.portal_link)}" target="_blank" style="color:var(--ifm-color-primary);font-weight:600">{_portal_label(f.portal_link)} &#8599;</a></p>' if f.portal_link else ""
            evidence_block = ""
            if show_evidence and f.evidence and f.evidence.get("field") != "current_state":
                ev = f.evidence
                ev_val = _esc(_json_mod.dumps(ev["value"], default=str) if not isinstance(ev.get("value"), str) else str(ev["value"]))
                evidence_block = f'<div style="background:rgba(245,158,11,.1);border-left:4px solid var(--warn);padding:8px 12px;border-radius:0 6px 6px 0;margin:8px 0;font-size:12px"><strong>Evidence:</strong> <code>{_esc(str(ev["field"]))}</code> = <code>{ev_val}</code> <span style="font-size:11px;margin-left:8px;color:var(--ifm-color-emphasis-400)">({_esc(ev.get("source", ""))})</span></div>'
            details_block = ""
            if f.details and "findings" in f.details:
                details_block = _render_secret_details_html(f.details)
            if f.details and "items" in f.details:
                scan_items_html = _render_scan_items_html(f.details)
                details_block += f'<details style="margin-top:8px"><summary style="cursor:pointer;font-size:12px;color:var(--ifm-color-emphasis-400);font-weight:600">Scanned Items</summary>{scan_items_html}</details>'
            elif include_api_response and f.details:
                details_json = _esc(_details_str(f.details))
                details_block += f'<details style="margin-top:8px"><summary style="cursor:pointer;font-size:12px;color:var(--ifm-color-emphasis-400);font-weight:600">API Response Details</summary><pre style="background:var(--ifm-background-surface-color);border:1px solid var(--ifm-color-emphasis-200);border-radius:6px;padding:10px;font-size:11px;overflow-x:auto;margin-top:4px;white-space:pre-wrap;word-break:break-all">{details_json}</pre></details>'
            benefits_block = f'<div style="background:rgba(0,164,0,.08);border-left:4px solid var(--pass);padding:10px 14px;border-radius:0 6px 6px 0;margin:8px 0"><strong style="color:var(--pass)">Why it matters:</strong><br><span style="font-size:13px">{_esc(f.benefits)}</span></div>' if f.benefits else ""
            items += f'<div class="card" style="background:var(--ifm-background-surface-color);border:1px solid var(--ifm-color-emphasis-200);border-radius:8px;padding:16px;margin-bottom:12px"><div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px"><div><span style="font-size:11px;color:var(--ifm-color-emphasis-400);font-weight:600">{_esc(f.check_id)}</span><h3 style="font-size:15px;font-weight:600;margin:4px 0 0">{_esc(f.title)}</h3></div><div style="display:flex;gap:6px;flex-shrink:0">{_status_badge(f.status)} {_sev_badge(f.severity)}</div></div><p style="font-size:13px;margin:0 0 8px"><strong>Current:</strong> {_esc(f.current_state)}</p>{evidence_block}<p style="font-size:13px;margin:0 0 8px">{_esc(f.description)}</p>{benefits_block}{rec_block}{ref_link}{portal_link}{details_block}</div>'
        cat_content[cat] = cat_summary + items

    # ── Prioritised recommendations tab ──
    prio_items = _build_prioritised_recommendations(result.findings)
    prio_content = ""
    if prio_items:
        _PRIO_COLORS = {"P1": "var(--fail)", "P2": "var(--high)", "P3": "var(--warn)", "P4": "var(--ifm-color-emphasis-400)"}
        _prio_counts: dict[str, int] = {}
        for item in prio_items:
            prefix = item["priority_label"][:2]
            _prio_counts[prefix] = _prio_counts.get(prefix, 0) + 1
        prio_dist = ""
        for p in ["P1", "P2", "P3", "P4"]:
            cnt = _prio_counts.get(p, 0)
            if cnt:
                prio_dist += f'<div class="kpi-card"><div class="kpi-val" style="color:{_PRIO_COLORS[p]}">{cnt}</div><div class="kpi-label">{p}</div></div>'
        prio_rows = ""
        for item in prio_items:
            prefix = item["priority_label"][:2]
            pc = _PRIO_COLORS.get(prefix, "var(--ifm-color-emphasis-400)")
            prio_badge = _badge(item["priority_label"], "badge-fail" if prefix == "P1" else ("badge-high" if prefix == "P2" else ("badge-warn" if prefix == "P3" else "badge-na")))
            ref = f'<a href="{_esc(item["reference_url"])}" target="_blank" style="color:var(--info);font-size:12px">Docs</a>' if item["reference_url"] else ""
            portal = f'<a href="{_esc(item["portal_link"])}" target="_blank" style="color:var(--ifm-color-primary);font-size:12px;font-weight:600">{_portal_label(item["portal_link"])}&nbsp;&#8599;</a>' if item["portal_link"] else ""
            cost_td = ""
            if show_cost:
                cost_td = f'<td style="font-size:12px;text-align:right;white-space:nowrap;color:var(--warn)">${item["cost_low"]:,} &ndash; ${item["cost_high"]:,}</td>' if item.get("cost_low") else '<td style="font-size:11px;color:var(--ifm-color-emphasis-400);text-align:center">&mdash;</td>'
            prio_rows += f'<tr><td>{prio_badge}</td><td style="font-weight:700;text-align:center">{item["priority_score"]}</td><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(item["check_id"])}</td><td style="font-size:12px">{_esc(item["category"])}</td><td>{_sev_badge(item["severity"])}</td><td>{_status_badge(item["status"])}</td><td style="font-size:12px;white-space:nowrap">{_esc(item["effort"])}</td>{cost_td}<td>{_esc(item["title"])}</td><td style="font-size:12px">{_esc(item["recommendation"])}</td><td>{ref}</td><td>{portal}</td></tr>'
        cost_th = '<th>Est. Cost ($/mo)</th>' if show_cost else ''
        prio_content = f'<p style="font-size:13px;color:var(--ifm-color-emphasis-400);margin-bottom:16px">{len(prio_items)} actionable finding{"s" if len(prio_items) != 1 else ""} ranked by Priority Score.</p><div class="kpi-strip" style="margin-bottom:20px">{prio_dist}</div><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th>Priority</th><th class="sortable">Score</th><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th><th class="sortable">Status</th><th class="sortable">Effort</th>{cost_th}<th>Title</th><th>Recommendation</th><th>Ref</th><th>Portal</th></tr></thead><tbody>{prio_rows}</tbody></table><div class="pager"></div></div>'

    # ── API Endpoints tab ──
    ep_content = ""
    if result.endpoint_summary and result.endpoint_summary.get("endpoints"):
        ep = result.endpoint_summary
        ep_rows = ""
        for e in ep["endpoints"]:
            if e["status"] == "items":
                icon = '<span style="color:var(--pass);font-weight:700">&#10003;</span>'
                count_text, count_style = f'{e["items_count"]} item{"s" if e["items_count"] != 1 else ""}', "color:var(--pass);font-weight:600"
            elif e["status"] == "config":
                icon = '<span style="color:var(--warn);font-weight:700">&#9881;</span>'
                count_text, count_style = "config/settings", "color:var(--warn);font-weight:600"
            elif e["status"] == "error":
                icon = '<span style="color:var(--fail);font-weight:700">&#10007;</span>'
                count_text, count_style = f'HTTP {e["error_code"]}', "color:var(--fail);font-weight:600"
            else:
                icon = '<span style="color:var(--ifm-color-emphasis-400)">&#9675;</span>'
                count_text, count_style = "0 items", "color:var(--ifm-color-emphasis-400)"
            ep_rows += f'<tr><td>{icon}</td><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(e["endpoint"])}</td><td style="{count_style}">{count_text}</td></tr>'
        ep_content = f'<div class="kpi-strip" style="margin-bottom:20px"><div class="kpi-card"><div class="kpi-val" style="color:var(--pass)">{ep["with_items"]}</div><div class="kpi-label">With Items</div></div><div class="kpi-card"><div class="kpi-val" style="color:var(--warn)">{ep["config"]}</div><div class="kpi-label">Config</div></div><div class="kpi-card"><div class="kpi-val" style="color:var(--ifm-color-emphasis-400)">{ep["empty"]}</div><div class="kpi-label">Empty</div></div><div class="kpi-card"><div class="kpi-val" style="color:var(--fail)">{ep.get("error", 0)}</div><div class="kpi-label">Errors</div></div><div class="kpi-card"><div class="kpi-val" style="color:var(--info)">{ep["total"]}</div><div class="kpi-label">Total</div></div></div><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th style="width:30px"></th><th>Endpoint</th><th>Result</th></tr></thead><tbody>{ep_rows}</tbody></table><div class="pager"></div></div>'

    # ── All Checks Reference tab ──
    _cat_order_ref: list[str] = []
    _cat_checks_ref: dict[str, list[str]] = {}
    for cid, cdata in SAT_CHECKS.items():
        cat = cdata.get("category", "Other")
        if cat not in _cat_checks_ref:
            _cat_order_ref.append(cat)
            _cat_checks_ref[cat] = []
        _cat_checks_ref[cat].append(cid)
    ref_rows = ""
    _sev_sort = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    for cat in _cat_order_ref:
        sorted_ids = sorted(_cat_checks_ref[cat], key=lambda c: (_sev_sort.get(SAT_CHECKS[c].get("severity", "low"), 3), c))
        for cid in sorted_ids:
            ck = SAT_CHECKS[cid]
            ref_url = ck.get("reference_url", "")
            ref_link = f'<a href="{_esc(ref_url)}" target="_blank" style="color:var(--info);font-size:11px">Docs</a>' if ref_url else ""
            _effort_td = f'<td style="font-size:12px;white-space:nowrap">{_esc(_get_effort(cid))}</td>' if show_effort else ''
            ref_rows += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px;white-space:nowrap">{_esc(cid)}</td><td>{_esc(cat)}</td><td>{_sev_badge(ck.get("severity", "low"))}</td>{_effort_td}<td><strong>{_esc(ck.get("title", ""))}</strong></td><td style="font-size:12px">{_esc(ck.get("description", ""))}</td><td style="font-size:12px">{_esc(ck.get("recommendation", ""))}</td><td>{ref_link}</td></tr>'
    _sev_counts: dict[str, int] = {}
    for ck in SAT_CHECKS.values():
        s = ck.get("severity", "low")
        _sev_counts[s] = _sev_counts.get(s, 0) + 1
    sev_chips = " ".join(f'{_sev_badge(s)} <span style="font-size:13px;margin-right:12px">{_sev_counts.get(s, 0)}</span>' for s in ["critical", "high", "medium", "low"])
    effort_th4 = '<th class="sortable">Effort</th>' if show_effort else ''
    checks_ref_content = f'<div style="margin-bottom:16px;display:flex;gap:16px;align-items:center;flex-wrap:wrap"><div style="font-size:15px;font-weight:600">Total: {len(SAT_CHECKS)} checks</div>{sev_chips}</div><div style="overflow-x:auto"><table class="sortable-table"><thead><tr><th class="sortable">Check ID</th><th class="sortable">Category</th><th class="sortable">Severity</th>{effort_th4}<th>Title</th><th>Description</th><th>Recommendation</th><th>Docs</th></tr></thead><tbody>{ref_rows}</tbody></table><div class="pager"></div></div>'

    # ── Definitions tab ──
    definitions_content = _build_definitions_html(show_effort)

    # ── Build tabs list ──
    _CAT_ORDER = ["Identity & Access", "Network Security", "Data Protection", "Compute Security",
                  "SQL Warehouses", "Secrets & Credentials", "Audit & Logging", "Governance",
                  "AI / ML Governance", "Informational", "Secret Scanning", "Operations"]
    tabs: list[tuple[str, str, str]] = [
        ("summary", "Summary", summary_content),
        ("all-findings", f"All Findings ({result.total_checks})", all_findings_content),
        ("failed-checks", f"{_pl(result.failed, 'Failed Check')}", failed_content),
        ("warnings", f"{_pl(result.warnings, 'Warning')}", warnings_content),
        ("api-errors", f"{_pl(result.api_errors, 'API Error')}", api_errors_content),
        ("na-checks", f"{_pl(result.not_applicable, 'N/A', 'N/As')}", na_content),
        ("passed-checks", f"{_pl(result.passed, 'Passed', 'Passed')}", passed_content),
    ]
    if prio_content:
        tabs.append(("prioritised", f"Prioritised ({len(prio_items)})", prio_content))

    # Category tabs — inserted after a "Categories" separator in the sidebar
    _cat_tab_start = len(tabs)
    _seen_cats: set[str] = set()
    for cat in _CAT_ORDER:
        if cat in cat_content:
            _seen_cats.add(cat)
            tid = "cat-" + _sanitize_name(cat)
            tabs.append((tid, cat, cat_content[cat]))
    for cat in sorted(cat_content.keys()):
        if cat not in _seen_cats:
            tid = "cat-" + _sanitize_name(cat)
            tabs.append((tid, cat, cat_content[cat]))

    if ep_content:
        tabs.append(("api-endpoints", "API Endpoints", ep_content))
    tabs.append(("checks-reference", f"All Checks ({len(SAT_CHECKS)})", checks_ref_content))
    tabs.append(("definitions", "Definitions", definitions_content))

    # ── Build sidebar + panels with prev/next ──
    sidebar_items = ""
    tab_panels = ""
    for i, (tid, label, content) in enumerate(tabs):
        active = " menu__link--active" if i == 0 else ""
        vis = " visible" if i == 0 else ""

        # Insert "Categories" separator before first category tab
        if i == _cat_tab_start and _cat_tab_start < len(tabs):
            sidebar_items += '<li class="menu__category">Categories <span class="menu__category-chevron">&#9662;</span></li>\n'

        sidebar_items += f'<li class="menu__list-item"><button class="menu__link{active}" data-tab="{tid}"><span>{_esc(label)}</span></button></li>\n'

        prev_next = '<nav class="pagination-nav">'
        if i > 0:
            prev_next += f'<a class="pagination-nav__link pagination-nav__link--prev" onclick="switchTab(\'{tabs[i-1][0]}\')"><span class="pagination-nav__sublabel">&#8249; Previous</span><span class="pagination-nav__label">{_esc(tabs[i-1][1])}</span></a>'
        if i < len(tabs) - 1:
            prev_next += f'<a class="pagination-nav__link pagination-nav__link--next" onclick="switchTab(\'{tabs[i+1][0]}\')"><span class="pagination-nav__sublabel">Next &#8250;</span><span class="pagination-nav__label">{_esc(tabs[i+1][1])}</span></a>'
        prev_next += '</nav>'

        tab_panels += f'<div class="tab-panel{vis}" id="panel-{tid}">\n  <div class="card">\n    <h2>{_esc(label)}</h2>\n    {content}\n  </div>\n  {prev_next}\n</div>\n'

    ws_name = result.workspace_name or "Workspace"

    # ── KPI strip ──
    kpi_strip = f"""<div class="kpi-strip">
<div class="kpi-card"><div class="kpi-label">Score</div><div class="kpi-val" style="color:{score_color}">{score}/100</div><div class="kpi-sub">{_esc(grade)}</div></div>
<div class="kpi-card"><div class="kpi-label">Total Checks</div><div class="kpi-val">{result.total_checks}</div><div class="kpi-sub">{len(_applicable)} scored</div></div>
<div class="kpi-card">{_score_gauge_svg(score, 70)}<div class="kpi-label" style="margin-top:4px">Compliance</div></div>
<div class="kpi-card"><div class="kpi-label">Passed</div><div class="kpi-val" style="color:var(--pass)">{result.passed}</div></div>
<div class="kpi-card"><div class="kpi-label">Failed</div><div class="kpi-val" style="color:var(--fail)">{result.failed}</div></div>
<div class="kpi-card"><div class="kpi-label">Warnings</div><div class="kpi-val" style="color:var(--warn)">{result.warnings}</div></div>"""
    if result.not_applicable:
        kpi_strip += f'<div class="kpi-card"><div class="kpi-label">N/A</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-400)">{result.not_applicable}</div></div>'
    if result.api_errors:
        kpi_strip += f'<div class="kpi-card"><div class="kpi-label">API Errors</div><div class="kpi-val" style="color:var(--ifm-color-primary)">{result.api_errors}</div></div>'
    kpi_strip += "</div>"

    # ── Full HTML ──
    back_link_html = (f'<a href="{_esc(summary_link)}" style="color:var(--ifm-color-primary-light);text-decoration:none;'
                      f'font-size:0.85rem;display:inline-flex;align-items:center;gap:0.4rem;margin-bottom:1rem">'
                      f'&#8249; Back to Summary</a>') if summary_link else ""
    html_doc = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SAT Scanner &mdash; {_esc(ws_name)}</title>
<style>{_MODERN_CSS}</style><script>try{{var t=localStorage.getItem('satscanner-theme');if(t)document.documentElement.setAttribute('data-theme',t);}}catch(e){{}}</script>
</head><body>
<nav class="navbar">
  <button class="sidebar-toggle" id="sidebar-toggle-btn" onclick="toggleSidebar()" title="Hide sidebar"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg></button>
  <a class="navbar__brand" href="#">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
    SAT Scanner
  </a>
  <div class="navbar__items">
    <span class="navbar__link navbar__link--active">Docs</span>
    <span class="navbar__link" style="color:var(--ifm-color-emphasis-400);cursor:default">{_esc(ws_name)}</span>
    <span class="navbar__version">v{__version__}</span>
    <span class="navbar__search"><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="7" cy="7" r="5"/><line x1="11" y1="11" x2="15" y2="15"/></svg>Search<kbd>&#8984;K</kbd></span>
    <button class="theme-toggle" id="theme-toggle-btn" onclick="toggleTheme()" title="Switch to light mode"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg></button>
  </div>
</nav>
<div class="layout">
  <aside class="sidebar">
    <ul class="menu__list">
      <li class="menu__list-item"><div class="menu__category">Dashboard <span class="menu__category-chevron">&#9662;</span></div></li>
      {sidebar_items}
    </ul>
  </aside>
  <main class="main">
    {back_link_html}
    <nav class="breadcrumb">
      <a class="breadcrumb__home" href="#"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></a>
      <span class="breadcrumb__sep">&#8250;</span>
      <span class="breadcrumb__item">{_esc(ws_name)}</span>
      <span class="breadcrumb__sep">&#8250;</span>
      <span class="breadcrumb__current" id="breadcrumb-current">Overview</span>
    </nav>
    <div class="landing-hero">
      <div class="landing-hero__title">{_esc(ws_name)}</div>
      <div class="landing-hero__sub">{_esc(result.workspace_url)}</div>
      <div class="landing-hero__tagline">Scanned {_esc(ts)}</div>
    </div>
    {kpi_strip}
    <div class="search-wrap"><input type="text" id="searchInput" placeholder="Search checks, categories, keywords..." oninput="doSearch(this.value)"><span id="searchCount" class="search-count"></span></div>
    {tab_panels}
  </main>
  <aside class="toc-sidebar">
    <div class="toc-sidebar__title">On this page</div>
    <ul class="toc-sidebar__list" id="toc-list"></ul>
  </aside>
</div>
<button class="toc-toggle" id="toc-toggle-btn" onclick="toggleTOC()" title="Hide table of contents">&#10005;</button>
<footer class="footer">
  <div class="footer__title">SAT Scanner</div>
  <div class="footer__links"><span class="footer__link">Modern Profile</span><span class="footer__link">v{__version__}</span></div>
  <div class="footer__copyright">Generated by SAT Scanner</div>
</footer>
<script>{_MODERN_JS}</script>
</body></html>"""

    ws_fname = _sanitize_name(result.workspace_name) if result.workspace_name else "workspace"
    path = output_dir / f"{ws_fname}.html"
    path.write_text(html_doc, encoding="utf-8")
    return str(path)

def export_recommendation_summary_modern(
    result: SATScanResult | None, output_dir: Path,
    show_cost: bool = False, findings: list[SATFinding] | None = None,
    show_architecture: bool = False,
) -> str:
    """Generate a modern SchemaX/Docusaurus-style Recommendation Summary HTML report.

    Pass ``findings`` directly for combined multi-workspace reports (result can be None).
    """
    import json as _json_mod

    src = findings if findings is not None else (result.findings if result else [])
    prio_items = _build_prioritised_recommendations(src)
    if not prio_items:
        return ""

    # ── Deduplicate by check_id ──
    _STATUS_RANK = {"FAIL": 2, "WARN": 1}
    _dedup: dict[str, dict] = {}
    for item in prio_items:
        cid = item["check_id"]
        if cid not in _dedup:
            _dedup[cid] = {**item, "_resource_count": 1}
        else:
            _dedup[cid]["_resource_count"] += 1
            if _STATUS_RANK.get(item["status"], 0) > _STATUS_RANK.get(_dedup[cid]["status"], 0):
                _dedup[cid]["status"] = item["status"]
            if item["priority_score"] > _dedup[cid]["priority_score"]:
                _dedup[cid]["priority_score"] = item["priority_score"]
                _dedup[cid]["priority_label"] = item["priority_label"]
    prio_items = sorted(_dedup.values(), key=lambda x: (-x["priority_score"], x["check_id"]))

    if not prio_items:
        return ""

    output_dir.mkdir(parents=True, exist_ok=True)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    date_str = datetime.now().strftime("%Y-%m-%d")

    # ── Effort badge helper ──
    _EFFORT_CLASSES = {
        "Quick Fix (5\u201315 min)": "badge-pass",
        "Moderate (1\u20134 hrs)": "badge-warn",
        "Significant (1\u20133 days)": "badge-high",
        "Project (1+ weeks)": "badge-na",
    }

    def _effort_badge(eff: str) -> str:
        cls = _EFFORT_CLASSES.get(eff, "badge-na")
        return f'<span class="badge {cls}">{_esc(eff)}</span>'

    # ── Counts ──
    total = len(prio_items)
    fail_count = sum(1 for i in prio_items if i["status"] == "FAIL")
    warn_count = sum(1 for i in prio_items if i["status"] == "WARN")

    # Group by priority bucket
    _PRIO_BUCKETS: dict[str, list[dict]] = {"P1": [], "P2": [], "P3": [], "P4": []}
    for item in prio_items:
        bucket = item["priority_label"][:2]
        _PRIO_BUCKETS.setdefault(bucket, []).append(item)

    p1_count = len(_PRIO_BUCKETS["P1"])
    p2_count = len(_PRIO_BUCKETS["P2"])
    p3_count = len(_PRIO_BUCKETS["P3"])
    p4_count = len(_PRIO_BUCKETS["P4"])

    # ── Category Breakdown ──
    cat_stats: dict[str, dict[str, int]] = {}
    for item in prio_items:
        cat = item["category"]
        if cat not in cat_stats:
            cat_stats[cat] = {"fail": 0, "warn": 0}
        if item["status"] == "FAIL":
            cat_stats[cat]["fail"] += 1
        else:
            cat_stats[cat]["warn"] += 1
    sorted_cats = sorted(cat_stats.items(), key=lambda x: x[1]["fail"] + x[1]["warn"], reverse=True)

    cat_rows = ""
    for cat, counts in sorted_cats:
        cat_total = counts["fail"] + counts["warn"]
        pct = max(2, round(counts["fail"] / cat_total * 100)) if cat_total else 0
        cat_rows += (
            f'<tr><td style="font-weight:600">{_esc(cat)}</td>'
            f'<td style="text-align:center"><span class="badge badge-fail">{counts["fail"]}</span></td>'
            f'<td style="text-align:center"><span class="badge badge-warn">{counts["warn"]}</span></td>'
            f'<td style="text-align:center;font-weight:700">{cat_total}</td>'
            f'<td><div style="background:var(--ifm-color-emphasis-200);border-radius:4px;height:8px;width:100%">'
            f'<div style="background:var(--fail);border-radius:4px;height:8px;width:{pct}%"></div></div></td></tr>'
        )

    # ── Effort Breakdown ──
    effort_counts: dict[str, int] = {}
    for item in prio_items:
        eff = item.get("effort", "Moderate (1\u20134 hrs)")
        effort_counts[eff] = effort_counts.get(eff, 0) + 1
    effort_order = ["Quick Fix (5\u201315 min)", "Moderate (1\u20134 hrs)", "Significant (1\u20133 days)", "Project (1+ weeks)"]
    effort_rows = ""
    for eff in effort_order:
        cnt = effort_counts.get(eff, 0)
        if cnt:
            effort_rows += (
                f'<tr><td>{_effort_badge(eff)}</td>'
                f'<td style="text-align:center;font-weight:700;font-size:18px">{cnt}</td></tr>'
            )

    # ── Cost summary for overview (optional) ──
    cost_kpi = ""
    if show_cost:
        total_low = sum(i["cost_low"] for i in prio_items if i.get("cost_low"))
        total_high = sum(i["cost_high"] for i in prio_items if i.get("cost_high"))
        if total_low:
            cost_kpi = (
                f'<div class="kpi-card"><div class="kpi-label">Est. Monthly Cost</div>'
                f'<div class="kpi-val" style="color:var(--warn)">${total_low:,} &ndash; ${total_high:,}</div></div>'
            )

    # ── Overview content ──
    overview = f"""<div class="kpi-strip">
<div class="kpi-card"><div class="kpi-label">Total Findings</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-600)">{total}</div></div>
<div class="kpi-card"><div class="kpi-label">Failures</div><div class="kpi-val" style="color:var(--fail)">{fail_count}</div></div>
<div class="kpi-card"><div class="kpi-label">Warnings</div><div class="kpi-val" style="color:var(--warn)">{warn_count}</div></div>
<div class="kpi-card"><div class="kpi-label">P1 - Immediate</div><div class="kpi-val" style="color:var(--fail)">{p1_count}</div></div>
<div class="kpi-card"><div class="kpi-label">P2 - This Sprint</div><div class="kpi-val" style="color:var(--high)">{p2_count}</div></div>
<div class="kpi-card"><div class="kpi-label">P3 - Next Sprint</div><div class="kpi-val" style="color:var(--warn)">{p3_count}</div></div>
<div class="kpi-card"><div class="kpi-label">P4 - Backlog</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-400)">{p4_count}</div></div>
{cost_kpi}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:24px">
<div class="card">
<h3>Category Breakdown</h3>
<table class="sortable-table"><thead><tr><th>Category</th><th style="text-align:center">Fail</th><th style="text-align:center">Warn</th><th style="text-align:center">Total</th><th style="width:120px">Fail %</th></tr></thead>
<tbody>{cat_rows}</tbody></table>
</div>
<div class="card">
<h3>Effort Breakdown</h3>
<table class="sortable-table"><thead><tr><th>Effort Level</th><th style="text-align:center">Count</th></tr></thead>
<tbody>{effort_rows}</tbody></table>
</div>
</div>"""

    # ── P1-P4 tab content ──
    _PRIO_LABELS = {
        "P1": ("Fix Immediately", "var(--fail)", "Critical and high-severity findings that pose immediate security risk."),
        "P2": ("Fix This Sprint", "var(--high)", "High and medium-severity findings to address within the current sprint."),
        "P3": ("Plan for Next Sprint", "var(--warn)", "Medium-severity findings and architectural improvements for upcoming sprints."),
        "P4": ("Backlog", "var(--ifm-color-emphasis-400)", "Low-severity findings and best-practice improvements to schedule when capacity allows."),
    }

    prio_tab_contents: dict[str, str] = {}
    for px in ["P1", "P2", "P3", "P4"]:
        items_px = _PRIO_BUCKETS[px]
        _, color, desc = _PRIO_LABELS[px]
        if not items_px:
            panel_body = f'<div class="card" style="border-left:4px solid {color}"><p style="color:var(--ifm-color-emphasis-400);font-style:italic;padding:12px">No findings in this priority level.</p></div>'
        else:
            by_cat: dict[str, list[dict]] = {}
            for itm in items_px:
                by_cat.setdefault(itm["category"], []).append(itm)

            panel_body = f'<div class="card" style="border-left:4px solid {color};padding:12px 16px;margin-bottom:20px;font-size:13px">{desc}</div>'
            for cat, cat_items in by_cat.items():
                panel_body += (
                    f'<div class="card"><h3 style="display:flex;align-items:center;justify-content:space-between">'
                    f'<span>{_esc(cat)}</span>'
                    f'<button class="jira-story-btn" data-category="{_esc(cat)}" '
                    f'style="font-size:11px;padding:3px 10px;border:1px solid var(--ifm-color-primary);color:var(--ifm-color-primary);'
                    f'background:transparent;border-radius:4px;cursor:pointer;display:none" '
                    f'title="Copy Story details">&#128203; Copy Story</button>'
                    f'</h3>'
                )
                cost_th = '<th style="width:120px">Est. Cost</th>' if show_cost else ''
                panel_body += (
                    f'<div style="overflow-x:auto"><table class="sortable-table"><thead><tr>'
                    f'<th class="sortable" style="width:110px">Check ID</th><th class="sortable" style="width:80px">Severity</th>'
                    f'<th class="sortable" style="width:70px">Status</th><th class="sortable" style="width:140px">Effort</th>{cost_th}'
                    f'<th>Title</th></tr></thead><tbody>'
                )
                for itm in cat_items:
                    cost_td = ""
                    if show_cost:
                        if itm.get("cost_low"):
                            cost_td = f'<td style="font-size:12px;color:var(--warn);white-space:nowrap">${itm["cost_low"]:,} &ndash; ${itm["cost_high"]:,}/mo</td>'
                        else:
                            cost_td = '<td style="font-size:11px;color:var(--ifm-color-emphasis-400);text-align:center">&mdash;</td>'
                    cur_state = itm.get("current_state", "")
                    cur_state_html = (
                        f'<div style="font-size:11px;color:var(--ifm-color-emphasis-400);margin-top:2px;font-weight:400">{_esc(cur_state)}</div>'
                        if cur_state else ''
                    )
                    panel_body += (
                        f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(itm["check_id"])}</td>'
                        f'<td>{_sev_badge(itm["severity"])}</td>'
                        f'<td>{_status_badge(itm["status"])}</td>'
                        f'<td>{_effort_badge(itm.get("effort", "Moderate (1\u20134 hrs)"))}</td>'
                        f'{cost_td}'
                        f'<td style="font-weight:500">{_esc(itm["title"])}{cur_state_html}</td></tr>'
                    )
                panel_body += '</tbody></table><div class="pager"></div></div>'

                # Expandable details per item
                for itm in cat_items:
                    rec = itm.get("recommendation", "")
                    why = itm.get("benefits", "")
                    cost_detail = ""
                    if show_cost and itm.get("cost_reason"):
                        cost_detail = f'<p><strong>Estimated cost impact:</strong> {_esc(itm["cost_reason"])}</p>'
                    _jd = _json_mod.dumps({
                        "check_id": itm["check_id"], "category": itm["category"],
                        "severity": itm["severity"], "status": itm["status"],
                        "title": itm["title"], "current_state": itm.get("current_state", ""),
                        "recommendation": rec, "benefits": why,
                        "priority_label": itm.get("priority_label", ""),
                        "effort": itm.get("effort", ""),
                        "reference_url": itm.get("reference_url", ""),
                        "remediation_plan": itm.get("remediation_plan", {}),
                    }).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
                    panel_body += (
                        f'<details style="margin:6px 0 12px 8px;font-size:13px" data-jira="{_jd}">'
                        f'<summary style="cursor:pointer;font-weight:600;color:var(--ifm-color-primary);display:flex;align-items:center;justify-content:space-between">'
                        f'<span><code>{_esc(itm["check_id"])}</code> &mdash; {_esc(itm["title"])}</span>'
                        f'<button class="jira-task-btn" '
                        f'style="font-size:10px;padding:2px 8px;border:1px solid var(--pass);color:var(--pass);'
                        f'background:transparent;border-radius:4px;cursor:pointer;display:none;margin-left:8px" '
                        f'title="Copy Task details">&#128203; Copy Task</button>'
                        f'</summary>'
                        f'<div style="padding:8px 16px;background:var(--ifm-background-surface-color);border-radius:6px;margin-top:4px">'
                    )
                    cur_state_detail = itm.get("current_state", "")
                    if cur_state_detail:
                        panel_body += f'<p><strong>Finding Details:</strong> {_esc(cur_state_detail)}</p>'
                    panel_body += f'<p><strong>Recommendation:</strong> {_esc(rec)}</p>'
                    if why:
                        panel_body += f'<p><strong>Why it matters:</strong> {_esc(why)}</p>'
                    panel_body += f'{cost_detail}</div></details>'
                panel_body += '</div>'

        prio_tab_contents[px] = panel_body

    unique_cats = len(cat_stats)

    # ── Roadmap panel ──
    from ...remediation import build_remediation_timeline
    timeline = build_remediation_timeline(prio_items)

    _phase_colors_css = {"P1": "var(--fail)", "P2": "var(--high)", "P3": "var(--warn)", "P4": "var(--ifm-color-emphasis-400)"}

    roadmap_content = '<div class="card" style="border-left:4px solid var(--pass);padding:12px 16px;margin-bottom:20px;font-size:13px">Remediation roadmap grouped by priority phase and category. Effort estimates help plan sprint capacity.</div>'
    ts = timeline["summary"]
    roadmap_content += '<div class="kpi-strip" style="margin-bottom:24px">'
    roadmap_content += f'<div class="kpi-card"><div class="kpi-label">Total Findings</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-600)">{ts["total_findings"]}</div></div>'
    roadmap_content += f'<div class="kpi-card"><div class="kpi-label">Total Effort</div><div class="kpi-val" style="color:var(--pass)">{ts["total_effort_hours"]}h</div></div>'
    roadmap_content += f'<div class="kpi-card"><div class="kpi-label">Working Days</div><div class="kpi-val" style="color:var(--ifm-color-primary)">{ts["total_working_days"]}d</div></div>'
    roadmap_content += f'<div class="kpi-card"><div class="kpi-label">Est. Resources</div><div class="kpi-val" style="color:var(--info)">{ts["estimated_resources"]}</div></div>'
    roadmap_content += f'<div class="kpi-card"><div class="kpi-label">Categories</div><div class="kpi-val" style="color:var(--ifm-color-primary-light)">{ts["categories"]}</div></div>'
    for phase in timeline["phases"]:
        ppx = phase["priority"]
        pc = _phase_colors_css.get(ppx, "var(--ifm-color-emphasis-400)")
        _res_label = f' / {phase["estimated_resources"]} res' if phase["estimated_resources"] else ''
        roadmap_content += f'<div class="kpi-card"><div class="kpi-label">{_esc(phase["phase"][:20])}</div><div class="kpi-val" style="color:{pc}">{phase["total_working_days"]}d{_res_label}</div></div>'
    roadmap_content += '</div>'
    roadmap_content += '<div class="card" style="border-left:4px solid var(--info);padding:10px 14px;margin-bottom:16px;font-size:12px"><strong>Resource Estimation:</strong> Based on 8 hours/working day. P1: 1 wk, P2: 2 wks, P3: 5 wks, P4: flexible.</div>'
    for phase in timeline["phases"]:
        ppx = phase["priority"]
        pc = _phase_colors_css.get(ppx, "var(--ifm-color-emphasis-400)")
        _res_txt = f' &bull; {phase["estimated_resources"]} resource(s)' if phase["estimated_resources"] else ''
        roadmap_content += f'<h3 style="border-left:4px solid {pc};padding-left:12px">{_esc(phase["phase"])} &mdash; {phase["total_working_days"]}d ({phase["total_hours"]}h){_res_txt}</h3>'
        if not phase["categories"]:
            roadmap_content += '<p style="color:var(--ifm-color-emphasis-400);font-style:italic;padding:8px 16px">No findings in this phase.</p>'
            continue
        for cat_rm, cat_data in phase["categories"].items():
            roadmap_content += '<details style="margin:8px 0 12px 8px;font-size:13px" open>'
            roadmap_content += f'<summary style="cursor:pointer;font-weight:600;color:var(--ifm-color-primary);padding:4px 0">{_esc(cat_rm)} &mdash; {cat_data["subtotal_working_days"]}d ({cat_data["subtotal_hours"]}h) &bull; {len(cat_data["findings"])} findings</summary>'
            roadmap_content += '<table class="sortable-table" style="margin:8px 0"><thead><tr><th>Check ID</th><th>Title</th><th>Severity</th><th>Effort</th><th>Est. Hours</th><th>Working Days</th></tr></thead><tbody>'
            for f_rm in cat_data["findings"]:
                roadmap_content += f'<tr><td style="font-family:var(--ifm-font-family-monospace);font-size:12px">{_esc(f_rm["check_id"])}</td><td style="font-weight:500">{_esc(f_rm["title"])}</td><td>{_sev_badge(f_rm["severity"])}</td><td style="font-size:12px">{_esc(f_rm["effort"])}</td><td style="text-align:center;font-weight:600">{f_rm["effort_hours"]}</td><td style="text-align:center;font-weight:600">{f_rm["working_days"]}</td></tr>'
            roadmap_content += '</tbody></table></details>'

    # ── Change Management panel ──
    change_content = '<div class="card" style="border-left:4px solid var(--ifm-color-primary);padding:12px 16px;margin-bottom:20px;font-size:13px">Change management templates with prerequisites, checklists, rollback plans, and approval requirements.</div>'
    for px in ["P1", "P2", "P3", "P4"]:
        px_items = [i for i in prio_items if i.get("priority_label", "").startswith(px)]
        if not px_items:
            continue
        _, pcolor, _ = _PRIO_LABELS[px]
        change_content += f'<h3 style="border-left:4px solid {pcolor};padding-left:12px">{px} Findings</h3>'
        for itm in px_items:
            plan = itm.get("remediation_plan", {})
            cl = plan.get("checklist", {})
            ia = plan.get("impact_assessment", {})
            cm = plan.get("change_management", {})
            ct_class = "badge-fail" if cm.get("change_type") == "Emergency" else ("badge-warn" if cm.get("change_type") == "Standard" else "badge-pass")
            change_content += '<details class="card" style="margin:8px 0 12px 0;font-size:13px;padding:0">'
            change_content += f'<summary style="cursor:pointer;font-weight:600;color:var(--ifm-color-primary);padding:12px 16px"><code>{_esc(itm["check_id"])}</code> &mdash; {_esc(itm["title"])}</summary>'
            change_content += '<div style="padding:12px 16px">'
            change_content += f'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px"><span class="badge {ct_class}">{_esc(cm.get("change_type", "Standard"))}</span>'
            change_content += f'<span class="badge {"badge-high" if cm.get("approval_required") else "badge-pass"}">{"Approval Required" if cm.get("approval_required") else "No Approval"}</span>'
            change_content += f'<span class="badge badge-na">{_esc(ia.get("downtime", "none"))} downtime</span>'
            change_content += f'<span class="badge badge-medium">{_esc(ia.get("blast_radius", "workspace"))} scope</span>'
            change_content += f'<span class="badge badge-na">{plan.get("estimated_duration_hours", "")}h est.</span></div>'
            if plan.get("prerequisites"):
                change_content += '<p style="font-weight:600;margin:8px 0 4px">Prerequisites</p><ul style="margin:0 0 8px 20px;padding:0">'
                for p in plan["prerequisites"]:
                    change_content += f'<li style="margin:2px 0">{_esc(p)}</li>'
                change_content += '</ul>'
            for section_name, section_key in [("Pre-Checks", "pre_checks"), ("Remediation Steps", "steps"), ("Post-Validation", "post_validation"), ("Rollback", "rollback")]:
                items_list = cl.get(section_key, [])
                if items_list:
                    change_content += f'<p style="font-weight:600;margin:8px 0 4px">{section_name}</p><ul style="margin:0 0 8px 20px;padding:0;list-style:none">'
                    for s in items_list:
                        icon = "&#9744; " if section_key != "rollback" else "&#8226; "
                        change_content += f'<li style="margin:2px 0">{icon}{_esc(s)}</li>'
                    change_content += '</ul>'
            change_content += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:8px;font-size:12px;color:var(--ifm-color-emphasis-400)">'
            change_content += f'<div><strong>Stakeholders:</strong> {_esc(", ".join(plan.get("stakeholders", [])))}</div>'
            change_content += f'<div><strong>Change Window:</strong> {_esc(cm.get("suggested_change_window", ""))}</div>'
            change_content += f'<div><strong>Testing:</strong> {_esc(cm.get("testing_plan", ""))}</div>'
            change_content += f'<div><strong>Communication:</strong> {_esc(cm.get("communication_plan", ""))}</div>'
            change_content += '</div></div></details>'

    # ── Architecture panel (opt-in) ──
    arch_content = ''
    if show_architecture:
        _total_checks = len(SAT_CHECKS)
        _cat_counts: dict[str, int] = {}
        _sev_counts: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for _ck in SAT_CHECKS.values():
            _cat_counts[_ck["category"]] = _cat_counts.get(_ck["category"], 0) + 1
            _sev_counts[_ck["severity"]] = _sev_counts.get(_ck["severity"], 0) + 1
        _n_cats = len(_cat_counts)
        arch_content = '<div class="card" style="border-left:4px solid var(--info);padding:12px 16px;margin-bottom:20px;font-size:13px">SAT Scanner architecture overview.</div>'
        arch_content += '<div class="kpi-strip" style="margin-bottom:24px">'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">Rules</div><div class="kpi-val" style="color:var(--ifm-color-primary)">{_total_checks}</div></div>'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">Critical</div><div class="kpi-val" style="color:var(--fail)">{_sev_counts["critical"]}</div></div>'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">High</div><div class="kpi-val" style="color:var(--high)">{_sev_counts["high"]}</div></div>'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">Medium</div><div class="kpi-val" style="color:var(--warn)">{_sev_counts["medium"]}</div></div>'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">Low</div><div class="kpi-val" style="color:var(--pass)">{_sev_counts["low"]}</div></div>'
        arch_content += f'<div class="kpi-card"><div class="kpi-label">Categories</div><div class="kpi-val" style="color:var(--ifm-color-primary-light)">{_n_cats}</div></div>'
        arch_content += '</div>'
        arch_content += '<p style="font-style:italic;color:var(--ifm-color-emphasis-400)">Architecture diagrams are available in the classic profile report.</p>'

    # ── Definitions panel ──
    definitions_content = """<div class="card" style="border-left:4px solid var(--info);padding:12px 16px;margin-bottom:20px;font-size:13px">Scoring methodology and priority definitions used in this report.</div>
<div class="card"><h3>Severity Weights</h3>
<table class="sortable-table"><thead><tr><th>Severity</th><th style="text-align:center">Weight</th><th>Description</th></tr></thead><tbody>
<tr><td><span class="badge badge-critical">CRITICAL</span></td><td style="text-align:center;font-weight:700">10</td><td>Immediate security risk requiring urgent remediation</td></tr>
<tr><td><span class="badge badge-high">HIGH</span></td><td style="text-align:center;font-weight:700">7</td><td>Significant risk that should be addressed quickly</td></tr>
<tr><td><span class="badge badge-medium">MEDIUM</span></td><td style="text-align:center;font-weight:700">4</td><td>Moderate risk, plan remediation in near-term sprints</td></tr>
<tr><td><span class="badge badge-low">LOW</span></td><td style="text-align:center;font-weight:700">2</td><td>Best practice improvement, schedule when capacity allows</td></tr>
</tbody></table></div>
<div class="card"><h3>Priority Levels</h3>
<table class="sortable-table"><thead><tr><th>Priority</th><th>Label</th><th>Description</th></tr></thead><tbody>
<tr><td style="font-weight:700;color:var(--fail)">P1</td><td>Fix Immediately</td><td>Critical/high severity with FAIL status and quick-fix effort. Address within 1 week.</td></tr>
<tr><td style="font-weight:700;color:var(--high)">P2</td><td>Fix This Sprint</td><td>High/medium severity failures. Address within current sprint (2 weeks).</td></tr>
<tr><td style="font-weight:700;color:var(--warn)">P3</td><td>Plan Next Sprint</td><td>Medium severity warnings and moderate-effort items. Plan for upcoming sprints.</td></tr>
<tr><td style="font-weight:700;color:var(--muted)">P4</td><td>Backlog</td><td>Low severity and best-practice improvements. Schedule when capacity allows.</td></tr>
</tbody></table></div>
<div class="card"><h3>Score Calculation</h3>
<p style="margin:8px 0;line-height:1.6">Score = <code>(1 - Total Penalty / Total Weight Pool) x 100</code></p>
<p style="margin:8px 0;line-height:1.6"><strong>FAIL</strong> = full severity weight as penalty. <strong>WARN</strong> = 50% of severity weight. <strong>PASS</strong> = no penalty.</p>
<table class="sortable-table"><thead><tr><th>Grade</th><th>Score Range</th><th>Interpretation</th></tr></thead><tbody>
<tr><td><span class="badge badge-pass">Good</span></td><td>80 &ndash; 100</td><td>Strong security posture with minor improvements</td></tr>
<tr><td><span class="badge badge-warn">Needs Improvement</span></td><td>60 &ndash; 79</td><td>Notable gaps that weaken security posture</td></tr>
<tr><td><span class="badge badge-fail">Critical</span></td><td>0 &ndash; 59</td><td>Serious vulnerabilities requiring immediate action</td></tr>
</tbody></table></div>"""

    # ── Build tabs list ──
    tabs: list[tuple[str, str, str, str]] = [
        ("overview", "Overview", f"{total}", overview),
        ("p1", "P1 \u2014 Fix Immediately", f"{p1_count}", prio_tab_contents["P1"]),
        ("p2", "P2 \u2014 Fix This Sprint", f"{p2_count}", prio_tab_contents["P2"]),
        ("p3", "P3 \u2014 Plan Next Sprint", f"{p3_count}", prio_tab_contents["P3"]),
        ("p4", "P4 \u2014 Backlog", f"{p4_count}", prio_tab_contents["P4"]),
        ("roadmap", "Roadmap", f'{ts["total_working_days"]}d', roadmap_content),
        ("change-mgmt", "Change Management", "", change_content),
    ]
    if show_architecture:
        tabs.append(("architecture", "Architecture", "", arch_content))
    tabs.append(("definitions", "Definitions", "", definitions_content))

    # ── Build sidebar + panels ──
    sidebar_items = ""
    tab_panels = ""
    for i, (tid, label, badge_text, content) in enumerate(tabs):
        active = " menu__link--active" if i == 0 else ""
        vis = " visible" if i == 0 else ""
        badge_html = f'<span class="menu__badge">{_esc(badge_text)}</span>' if badge_text else ''
        sidebar_items += f'<li class="menu__list-item"><button class="menu__link{active}" data-tab="{tid}"><span>{_esc(label)}</span>{badge_html}</button></li>\n'

        prev_next = '<nav class="pagination-nav">'
        if i > 0:
            prev_next += f'<a class="pagination-nav__link pagination-nav__link--prev" onclick="switchTab(\'{tabs[i-1][0]}\')"><span class="pagination-nav__sublabel">&#8249; Previous</span><span class="pagination-nav__label">{_esc(tabs[i-1][1])}</span></a>'
        if i < len(tabs) - 1:
            prev_next += f'<a class="pagination-nav__link pagination-nav__link--next" onclick="switchTab(\'{tabs[i+1][0]}\')"><span class="pagination-nav__sublabel">Next &#8250;</span><span class="pagination-nav__label">{_esc(tabs[i+1][1])}</span></a>'
        prev_next += '</nav>'

        tab_panels += f'<div class="tab-panel{vis}" id="panel-{tid}"><div class="card"><h2>{_esc(label)}</h2>{content}</div>{prev_next}</div>\n'

    first_tab_label = tabs[0][1] if tabs else "Overview"

    # ── Serialize findings for JS (Excel export) ──
    _excel_items = []
    for itm in prio_items:
        _excel_items.append({
            "check_id": itm.get("check_id", ""), "category": itm.get("category", ""),
            "severity": itm.get("severity", ""), "status": itm.get("status", ""),
            "title": itm.get("title", ""), "effort": itm.get("effort", ""),
            "current_state": itm.get("current_state", ""),
            "recommendation": itm.get("recommendation", ""),
            "benefits": itm.get("benefits", ""),
            "priority_label": itm.get("priority_label", ""),
            "reference_url": itm.get("reference_url", ""),
            "_resource_count": itm.get("_resource_count", 0),
            "remediation_plan": itm.get("remediation_plan", {}),
        })
    findings_json = _json_mod.dumps(_excel_items, ensure_ascii=True).replace("</", "<\\/")
    timeline_json = _json_mod.dumps(timeline, ensure_ascii=True).replace("</", "<\\/")

    # ── Build recommendation JS (Jira, ADO, Excel helpers) ──
    _rec_js = _build_sat_recommendation_js(findings_json, timeline_json, _esc(now_str), date_str)

    # ── Assemble full HTML document ──
    html_doc = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SAT Recommendations Summary</title>
<style>{_MODERN_CSS}</style><script>try{{var t=localStorage.getItem('satscanner-theme');if(t)document.documentElement.setAttribute('data-theme',t);}}catch(e){{}}</script>
<script src="https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js"></script>
</head><body>
<nav class="navbar">
  <button class="sidebar-toggle" id="sidebar-toggle-btn" onclick="toggleSidebar()" title="Hide sidebar"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg></button>
  <a class="navbar__brand" href="#">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
    SAT Scanner
  </a>
  <div class="navbar__items">
    <span class="navbar__link navbar__link--active">Docs</span>
    <span class="navbar__link" style="color:var(--ifm-color-emphasis-400);cursor:default">Recommendations</span>
    <span class="navbar__version">v{__version__}</span>
    <button class="theme-toggle" id="theme-toggle-btn" onclick="toggleTheme()" title="Switch to light mode"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg></button>
  </div>
</nav>
<div class="layout">
  <aside class="sidebar">
    <ul class="menu__list">
      <li class="menu__category">Dashboard <span class="menu__category-chevron">&#9662;</span></li>
      {sidebar_items}
    </ul>
  </aside>
  <main class="main">
    <nav class="breadcrumb">
      <a class="breadcrumb__home" href="#"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></a>
      <span class="breadcrumb__sep">&#8250;</span>
      <span class="breadcrumb__item">Recommendations</span>
      <span class="breadcrumb__sep">&#8250;</span>
      <span class="breadcrumb__current" id="breadcrumb-current">{_esc(first_tab_label)}</span>
    </nav>
    <div class="landing-hero">
      <div class="landing-hero__title">Recommendations Summary</div>
      <div class="landing-hero__tagline">{total} findings across {unique_cats} categories &middot; Generated {_esc(now_str)}</div>
    </div>
    <div class="search-wrap">
      <input type="text" id="searchInput" placeholder="Search across all tabs..." oninput="doSearch(this.value)">
      <span id="searchCount" class="search-count"></span>
    </div>
    {tab_panels}
  </main>
  <aside class="toc-sidebar">
    <div class="toc-sidebar__title">On this page</div>
    <ul class="toc-sidebar__list" id="toc-list"></ul>
  </aside>
</div>
<button class="toc-toggle" id="toc-toggle-btn" onclick="toggleTOC()" title="Hide table of contents">&#10005;</button>
<footer class="footer">
  <div class="footer__title">SAT Scanner</div>
  <div class="footer__links">
    <span class="footer__link">Recommendations Summary</span>
    <span class="footer__link">v{__version__}</span>
  </div>
  <div class="footer__copyright">Generated by SAT Scanner</div>
</footer>
<script>{_MODERN_JS}</script>
<script>{_rec_js}</script>
</body></html>"""

    path = output_dir / "Recommendation_Summary.html"
    path.write_text(html_doc, encoding="utf-8")
    return str(path)


def _build_sat_recommendation_js(findings_json: str, timeline_json: str, now_str: str, date_str: str) -> str:
    """Build the JavaScript for Excel export in the recommendation summary."""
    return (
        "var _ALL_FINDINGS=" + findings_json + ";\n"
        "var _TIMELINE=" + timeline_json + ";\n"
        r"""
/* Excel export */
(function(){
  var btn=document.getElementById('excel-export-btn');
  if(!btn)return;
  btn.addEventListener('click',function(){
    if(typeof XLSX==='undefined'){alert('SheetJS not loaded.');return;}
    var wb=XLSX.utils.book_new();
    // Summary sheet
    var sumData=[['SAT Recommendations Summary'],['Generated','""" + now_str + r"""'],[''],
      ['Total Findings',_ALL_FINDINGS.length],
      ['Failures',_ALL_FINDINGS.filter(function(f){return f.status==='FAIL'}).length],
      ['Warnings',_ALL_FINDINGS.filter(function(f){return f.status==='WARN'}).length]];
    var sumWs=XLSX.utils.aoa_to_sheet(sumData);
    XLSX.utils.book_append_sheet(wb,sumWs,'Summary');
    // All Findings sheet
    var hdr=['Check ID','Category','Severity','Status','Effort','Priority','Title','Current State','Recommendation','Benefits','Reference URL'];
    var rows=[hdr];
    _ALL_FINDINGS.forEach(function(f){
      rows.push([f.check_id,f.category,f.severity,f.status,f.effort,f.priority_label,f.title,f.current_state||'',f.recommendation||'',f.benefits||'',f.reference_url||'']);
    });
    var ws=XLSX.utils.aoa_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb,ws,'All Findings');
    // P1-P4 sheets
    ['P1','P2','P3','P4'].forEach(function(px){
      var items=_ALL_FINDINGS.filter(function(f){return(f.priority_label||'').indexOf(px)===0});
      if(!items.length)return;
      var r2=[hdr];
      items.forEach(function(f){r2.push([f.check_id,f.category,f.severity,f.status,f.effort,f.priority_label,f.title,f.current_state||'',f.recommendation||'',f.benefits||'',f.reference_url||'']);});
      var ws2=XLSX.utils.aoa_to_sheet(r2);
      XLSX.utils.book_append_sheet(wb,ws2,px+' Findings');
    });
    // Roadmap sheet
    if(_TIMELINE&&_TIMELINE.phases){
      var rRows=[['Phase','Category','Check ID','Title','Severity','Effort','Hours','Working Days']];
      _TIMELINE.phases.forEach(function(ph){
        var cats=ph.categories||{};
        Object.keys(cats).forEach(function(cat){
          (cats[cat].findings||[]).forEach(function(f){
            rRows.push([ph.phase,cat,f.check_id,f.title,f.severity,f.effort,f.effort_hours,f.working_days]);
          });
        });
      });
      var rWs=XLSX.utils.aoa_to_sheet(rRows);
      XLSX.utils.book_append_sheet(wb,rWs,'Roadmap');
    }
    XLSX.writeFile(wb,'SAT_Recommendations_""" + date_str + r""".xlsx');
  });
})();
"""
    )

def export_combined_html_modern(results: list[tuple[str, SATScanResult]], skipped: list[str],
                                avg_score: int, output_dir: Path, show_scan_items: bool = False,
                                show_effort: bool = False, show_cost: bool = False) -> None:
    """Generate a modern SchemaX/Docusaurus-style combined multi-workspace HTML report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = datetime.now().strftime("%Y-%m-%d")
    total_checks_all = sum(r.total_checks for _, r in results)
    total_passed = sum(r.passed for _, r in results)
    total_failed = sum(r.failed for _, r in results)
    total_warnings = sum(r.warnings for _, r in results)
    total_na = sum(r.not_applicable for _, r in results)
    total_api_errors = sum(r.api_errors for _, r in results)
    avg_color = "var(--pass)" if avg_score >= 80 else ("var(--warn)" if avg_score >= 60 else "var(--fail)")
    avg_grade = "Good" if avg_score >= 80 else ("Needs Improvement" if avg_score >= 60 else "Critical")

    def _ws_report_file(ws_name: str) -> str:
        sn = _sanitize_name(ws_name)
        fname = f"sat-{sn}-{date_str}.html" if sn else f"sat-{date_str}.html"
        return f"{sn}/{fname}" if len(results) > 1 and sn else fname

    # ── Summary tab ──
    skipped_html = ""
    if skipped:
        skipped_html = ("<div style='background:rgba(250,56,62,.1);border:1px solid rgba(250,56,62,.3);border-radius:var(--ifm-border-radius);padding:14px 18px;margin-bottom:18px;font-size:13px;color:var(--fail)'>" + f"<strong>&#9888; Skipped ({len(skipped)}):</strong> " + ", ".join(_esc(s) for s in skipped) + "</div>")
    ws_cards = ""
    for name, r in sorted(results, key=lambda x: x[1].overall_score):
        report_link = _ws_report_file(name)
        ws_cards += ("<div class='landing-capability-card'><div style='display:flex;justify-content:space-between;align-items:flex-start'><div style='flex:1;min-width:0'>" + f"<div class='landing-capability-card__title'>{_esc(name)}</div>" + f"<div style='font-size:12px;color:var(--ifm-color-emphasis-400);margin-bottom:8px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{_esc(r.workspace_url)}</div>" + "<div style='display:flex;gap:10px;flex-wrap:wrap;font-size:12px;color:var(--ifm-color-emphasis-400)'>" + f"<span>{r.total_checks} checks</span><span style='color:var(--pass)'>{_pl(r.passed, 'passed', 'passed')}</span>" + f"<span style='color:var(--fail)'>{_pl(r.failed, 'failed', 'failed')}</span><span style='color:var(--warn)'>{_pl(r.warnings, 'warning')}</span></div>" + f"<a href='{_esc(report_link)}' style='display:inline-block;margin-top:8px;font-size:12px;color:var(--ifm-color-primary-light);text-decoration:none;font-weight:600'>View Full Report &rarr;</a>" + f"</div><div style='flex-shrink:0;margin-left:12px'>{_score_gauge_svg(r.overall_score, 80)}</div></div></div>\n")
    summary_content = (skipped_html + f"<div style='text-align:center;margin-bottom:24px'>{_score_gauge_svg(avg_score, 140)}" + f"<div style='font-size:13px;color:var(--ifm-color-emphasis-400);margin-top:8px'>Average across {len(results)} workspace(s)</div></div>" + f"<h3 id='ws-scores'>Workspace Scores</h3><div class='landing-capabilities__grid'>{ws_cards}</div>")

    # ── Comparison tab ──
    all_cats: list[str] = []
    for _, r in results:
        for cat in r.category_scores:
            if cat not in all_cats:
                all_cats.append(cat)
    comp_header = "<th class='sortable'>Category</th>" + "".join(f"<th style='text-align:center;writing-mode:vertical-lr;text-orientation:mixed;max-height:120px;font-size:11px'>{_esc(n)}</th>" for n, _ in results)
    comp_rows = ""
    for cat in all_cats:
        comp_rows += f"<tr><td>{_esc(cat)}</td>"
        for _, r in results:
            cs = r.category_scores.get(cat, -1)
            if cs < 0:
                comp_rows += "<td style='text-align:center;color:var(--ifm-color-emphasis-300)'>&mdash;</td>"
            else:
                c = "var(--pass)" if cs >= 80 else ("var(--warn)" if cs >= 60 else "var(--fail)")
                comp_rows += f"<td style='text-align:center;font-weight:700;color:{c}'>{cs}</td>"
        comp_rows += "</tr>"
    comparison_content = f"<div style='overflow-x:auto'><table class='sortable-table'><thead><tr>{comp_header}</tr></thead><tbody>{comp_rows}</tbody></table></div>"

    # ── Common Issues tab ──
    check_status_map: dict[str, list[tuple[str, str, str, str, str]]] = {}
    for name, r in results:
        for f in r.findings:
            if f.status in ("FAIL", "WARN"):
                check_status_map.setdefault(f.check_id, []).append((name, f.status, f.title, f.severity, f.current_state))
    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    sorted_issues = sorted(check_status_map.items(), key=lambda x: (-len(x[1]), sev_order.get(x[1][0][3], 9)))
    if sorted_issues:
        issues_rows = ""
        for check_id, entries in sorted_issues:
            ws_names = ", ".join(e[0] for e in entries)
            title, sev = entries[0][2], entries[0][3]
            worst = "FAIL" if any(e[1] == "FAIL" for e in entries) else "WARN"
            ci_benefit = _esc(CHECK_BENEFITS.get(check_id, ""))
            issues_rows += f"<tr><td style='font-family:var(--ifm-font-family-monospace);font-size:12px'>{_esc(check_id)}</td><td>{_esc(title)}</td><td>{_sev_badge(sev)}</td><td>{_status_badge(worst)}</td><td style='font-weight:700;text-align:center'>{len(entries)}/{len(results)}</td><td style='font-size:12px'>{_esc(ws_names)}</td><td style='font-size:12px'>{ci_benefit}</td></tr>"
        common_issues_content = ("<p style='font-size:13px;color:var(--ifm-color-emphasis-400);margin-bottom:16px'>Checks that appear as FAIL or WARN across multiple workspaces.</p>" + f"<div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Check ID</th><th class='sortable'>Title</th><th class='sortable'>Severity</th><th class='sortable'>Worst Status</th><th class='sortable'>Affected</th><th class='sortable'>Workspaces</th><th>Why It Matters</th></tr></thead><tbody>{issues_rows}</tbody></table></div><div class='pager'></div>")
    else:
        common_issues_content = "<p style='color:var(--pass);font-weight:600'>No common issues found.</p>"

    # ── Per-workspace tabs ──
    _SWt = {"critical": 10, "high": 7, "medium": 4, "low": 2}
    ws_tab_content: dict[str, str] = {}
    for name, r in results:
        pass_rate = round(r.passed / max(r.total_checks, 1) * 100, 1)
        sc = "var(--pass)" if r.overall_score >= 80 else ("var(--warn)" if r.overall_score >= 60 else "var(--fail)")
        ws_kpis = (f"<div class='kpi-strip'><div class='kpi-card'><div class='kpi-label'>Score</div><div class='kpi-val' style='color:{sc}'>{r.overall_score}/100</div></div>" + f"<div class='kpi-card'><div class='kpi-label'>Checks</div><div class='kpi-val'>{r.total_checks}</div></div>" + f"<div class='kpi-card'><div class='kpi-label'>Passed</div><div class='kpi-val' style='color:var(--pass)'>{r.passed}</div><div class='kpi-sub'>{pass_rate}%</div></div>" + f"<div class='kpi-card'><div class='kpi-label'>Failed</div><div class='kpi-val' style='color:var(--fail)'>{r.failed}</div></div>" + f"<div class='kpi-card'><div class='kpi-label'>Warnings</div><div class='kpi-val' style='color:var(--warn)'>{r.warnings}</div></div>")
        if r.not_applicable:
            ws_kpis += f"<div class='kpi-card'><div class='kpi-label'>N/A</div><div class='kpi-val' style='color:var(--ifm-color-emphasis-400)'>{r.not_applicable}</div></div>"
        ws_kpis += "</div>"
        # Score breakdown
        _wa = [f for f in r.findings if not f.is_api_error and f.status != "NOT_APPLICABLE"]
        _wf = [f for f in _wa if f.status == "FAIL"]
        _ww = [f for f in _wa if f.status == "WARN"]
        _wp = [f for f in _wa if f.status == "PASS"]
        _wt = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _wa)
        _wfp = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _wf)
        _wwp = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) * 0.5 for f in _ww)
        _wtp = _wfp + _wwp
        ws_breakdown = (f"<div class='score-breakdown'><h3 style='font-size:13px;font-weight:700;margin-bottom:10px'>Score Breakdown</h3><table style='font-size:12px;border:none'>" + f"<tr><td style='border:none;padding:3px 10px 3px 0;color:var(--ifm-color-emphasis-400)'>Scored checks</td><td style='border:none;padding:3px 0;font-weight:600'>{len(_wa)} <span style='color:var(--ifm-color-emphasis-300);font-weight:400'>(excl. {r.not_applicable} N/A + {r.api_errors} API Error)</span></td></tr>" + f"<tr><td style='border:none;padding:3px 10px 3px 0;color:var(--ifm-color-emphasis-400)'>Total weight pool</td><td style='border:none;padding:3px 0;font-weight:600'>{_wt:.0f} pts</td></tr>" + f"<tr><td style='border:none;padding:3px 10px 3px 0;color:var(--fail)'>FAIL penalty ({len(_wf)} &times; full)</td><td style='border:none;padding:3px 0;font-weight:600;color:var(--fail)'>&minus;{_wfp:.0f} pts</td></tr>" + f"<tr><td style='border:none;padding:3px 10px 3px 0;color:var(--warn)'>WARN penalty ({len(_ww)} &times; half)</td><td style='border:none;padding:3px 0;font-weight:600;color:var(--warn)'>&minus;{_wwp:.1f} pts</td></tr>" + f"<tr><td style='border:none;padding:3px 10px 3px 0;color:var(--pass)'>PASS ({len(_wp)})</td><td style='border:none;padding:3px 0;font-weight:600;color:var(--pass)'>0 pts</td></tr>" + f"<tr style='border-top:1px solid var(--ifm-color-emphasis-200)'><td style='border:none;padding:6px 10px 3px 0;font-weight:600'>Formula</td><td style='border:none;padding:6px 0 3px;font-family:var(--ifm-font-family-monospace);font-size:11px'>(1 &minus; {_wtp:.1f} / {_wt:.0f}) &times; 100 = <strong style='color:{sc};font-size:13px'>{r.overall_score}</strong></td></tr></table></div>")
        cat_rows = ""
        for cat, cs in sorted(r.category_scores.items(), key=lambda x: x[1]):
            c = "var(--pass)" if cs >= 80 else ("var(--warn)" if cs >= 60 else "var(--fail)")
            cat_rows += f"<tr><td>{_esc(cat)}</td><td style='font-weight:700;color:{c};text-align:right'>{cs}</td><td style='width:200px'><div class='progress-bar'><div class='progress-fill' style='background:{c};width:{max(2, cs)}%'></div></div></td></tr>"
        ws_failed = [f for f in r.findings if f.status == "FAIL"]
        ws_warns_list = [f for f in r.findings if f.status == "WARN"]
        failed_html = ""
        if ws_failed:
            f_rows = ""
            _cfc = 8 if show_effort else 7
            for f in ws_failed:
                sdr = (f'<tr><td colspan="{_cfc}" style="padding:0 8px 12px">{_render_secret_details_html(f.details)}</td></tr>' if f.details and "findings" in f.details else "")
                sir = (f'<tr><td colspan="{_cfc}" style="padding:0 8px 12px">{_render_scan_items_html(f.details)}</td></tr>' if f.details and "items" in f.details else "")
                etd = f"<td style='font-size:12px;white-space:nowrap'>{_esc(f.effort)}</td>" if show_effort else ""
                f_rows += f"<tr><td style='font-family:var(--ifm-font-family-monospace);font-size:12px'>{_esc(f.check_id)}</td><td>{_sev_badge(f.severity)}</td>{etd}<td>{_esc(f.title)}</td><td style='font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{_esc(f.current_state)}</td><td style='font-size:12px'>{_esc(f.recommendation)}</td><td style='font-size:12px'>{_esc(f.benefits) if f.benefits else ''}</td></tr>{sdr}{sir}"
            eth = "<th>Effort</th>" if show_effort else ""
            failed_html = f"<h3 id='ws-failed' style='color:var(--fail)'>{_pl(len(ws_failed), 'Failed Check')}</h3><div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Check ID</th><th class='sortable'>Severity</th>{eth}<th class='sortable'>Title</th><th>Current State</th><th>Recommendation</th><th>Why It Matters</th></tr></thead><tbody>{f_rows}</tbody></table></div><div class='pager'></div>"
        else:
            failed_html = "<p style='color:var(--pass);font-size:13px;margin:16px 0;font-weight:600'>No failed checks.</p>"
        warn_html = ""
        if ws_warns_list:
            w_rows = ""
            for f in ws_warns_list:
                etd = f"<td style='font-size:12px;white-space:nowrap'>{_esc(f.effort)}</td>" if show_effort else ""
                w_rows += f"<tr><td style='font-family:var(--ifm-font-family-monospace);font-size:12px'>{_esc(f.check_id)}</td><td>{_sev_badge(f.severity)}</td>{etd}<td>{_esc(f.title)}</td><td style='font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{_esc(f.current_state)}</td><td style='font-size:12px'>{_esc(f.benefits) if f.benefits else ''}</td></tr>"
            eth = "<th>Effort</th>" if show_effort else ""
            warn_html = f"<h3 id='ws-warnings' style='color:var(--warn)'>{_pl(len(ws_warns_list), 'Warning')}</h3><div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Check ID</th><th class='sortable'>Severity</th>{eth}<th class='sortable'>Title</th><th>Current State</th><th>Why It Matters</th></tr></thead><tbody>{w_rows}</tbody></table></div><div class='pager'></div>"
        ws_report_link = _ws_report_file(name)
        ws_tab_content[name] = (f"<div style='text-align:center;margin-bottom:16px'>{_score_gauge_svg(r.overall_score, 120)}<div style='font-size:12px;color:var(--ifm-color-emphasis-400)'>{_esc(r.workspace_url)}</div><a href='{_esc(ws_report_link)}' style='display:inline-block;margin-top:6px;font-size:13px;color:var(--ifm-color-primary-light);text-decoration:none;font-weight:600'>Open Full Workspace Report &rarr;</a></div>" + f"{ws_kpis}{ws_breakdown}<h3 id='ws-categories'>Category Scores</h3><div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Category</th><th class='sortable' style='text-align:right'>Score</th><th style='width:200px'>Progress</th></tr></thead><tbody>{cat_rows}</tbody></table></div>{failed_html}{warn_html}")

    # ── Prioritised tab (combined) ──
    all_findings_combined: list[SATFinding] = []
    for _, r in results:
        all_findings_combined.extend(r.findings)
    _raw_prio = _build_prioritised_recommendations(all_findings_combined)
    _SR = {"FAIL": 2, "WARN": 1}
    _dd: dict[str, dict] = {}
    for item in _raw_prio:
        cid = item["check_id"]
        if cid not in _dd:
            _dd[cid] = {**item, "_ws_count": 1}
        else:
            _dd[cid]["_ws_count"] += 1
            if _SR.get(item["status"], 0) > _SR.get(_dd[cid]["status"], 0):
                _dd[cid]["status"] = item["status"]
            if item["priority_score"] > _dd[cid]["priority_score"]:
                _dd[cid]["priority_score"] = item["priority_score"]
                _dd[cid]["priority_label"] = item["priority_label"]
    comb_prio = sorted(_dd.values(), key=lambda x: x["priority_score"], reverse=True)
    prio_content = ""
    if comb_prio:
        _CPC = {"P1": "var(--fail)", "P2": "var(--high)", "P3": "var(--warn)", "P4": "var(--ifm-color-emphasis-400)"}
        _cpcnt: dict[str, int] = {}
        for item in comb_prio:
            px = item["priority_label"][:2]
            _cpcnt[px] = _cpcnt.get(px, 0) + 1
        cpd = "".join(f"<div style='background:var(--ifm-background-surface-color);border:1px solid var(--ifm-color-emphasis-200);border-radius:var(--ifm-border-radius);padding:12px 18px;text-align:center'><div style='font-size:24px;font-weight:700;color:{_CPC[p]}'>{_cpcnt[p]}</div><div style='font-size:11px;color:var(--ifm-color-emphasis-400);text-transform:uppercase'>{p}</div></div>" for p in ["P1", "P2", "P3", "P4"] if _cpcnt.get(p, 0))
        pr_rows = ""
        for item in comb_prio:
            px = item["priority_label"][:2]
            pc = _CPC.get(px, "var(--ifm-color-emphasis-400)")
            bc = "badge-fail" if px == "P1" else ("badge-high" if px == "P2" else ("badge-warn" if px == "P3" else "badge-na"))
            pb = _badge(item["priority_label"], bc)
            etd = f"<td style='font-size:12px;white-space:nowrap'>{_esc(item['effort'])}</td>" if show_effort else ""
            ctd = ""
            if show_cost:
                ctd = (f"<td style='font-size:12px;text-align:right;white-space:nowrap;color:var(--warn)'>${item['cost_low']:,} &ndash; ${item['cost_high']:,}</td>" if item.get("cost_low") else "<td style='font-size:11px;color:var(--ifm-color-emphasis-300);text-align:center'>&mdash;</td>")
            pr_rows += f"<tr><td>{pb}</td><td style='font-weight:700;color:{pc};text-align:center'>{item['priority_score']}</td><td style='font-family:var(--ifm-font-family-monospace);font-size:12px'>{_esc(item['check_id'])}</td><td style='font-size:12px'>{_esc(item['category'])}</td><td>{_sev_badge(item['severity'])}</td><td>{_status_badge(item['status'])}</td>{etd}{ctd}<td style='text-align:center;font-weight:600'>{item['_ws_count']}/{len(results)}</td><td>{_esc(item['title'])}</td><td style='font-size:12px'>{_esc(item['recommendation'])}</td><td style='font-size:12px'>{_esc(item.get('benefits', '') or '')}</td></tr>"
        eth = "<th class='sortable'>Effort</th>" if show_effort else ""
        cth = "<th class='sortable'>Est. Cost</th>" if show_cost else ""
        prio_content = (f"<p style='font-size:13px;color:var(--ifm-color-emphasis-400);margin-bottom:16px'>{len(comb_prio)} unique actionable check{'s' if len(comb_prio) != 1 else ''} across {len(results)} workspace(s), ranked by <strong>Priority Score</strong>.</p>" + f"<div style='display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap'>{cpd}</div>" + f"<div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Priority</th><th class='sortable'>Score</th><th class='sortable'>Check ID</th><th class='sortable'>Category</th><th class='sortable'>Severity</th><th class='sortable'>Status</th>{eth}{cth}<th class='sortable'>Workspaces</th><th class='sortable'>Title</th><th>Recommendation</th><th>Why It Matters</th></tr></thead><tbody>{pr_rows}</tbody></table></div><div class='pager'></div>")

    # ── API Endpoints tab (combined) ──
    all_ep_summaries = [r.endpoint_summary for _, r in results if r.endpoint_summary and r.endpoint_summary.get("endpoints")]
    ep_content = ""
    if all_ep_summaries:
        merged_ep: dict[str, dict] = {}
        for ep_sum in all_ep_summaries:
            for e in ep_sum["endpoints"]:
                key = e["endpoint"]
                if key not in merged_ep:
                    merged_ep[key] = {"endpoint": key, "status": e["status"], "items_count": e["items_count"], "error_code": e.get("error_code", 0)}
                else:
                    ex = merged_ep[key]
                    ex["items_count"] = max(ex["items_count"], e["items_count"])
                    if e["status"] == "items":
                        ex["status"] = "items"
                    elif e["status"] == "config" and ex["status"] not in ("items",):
                        ex["status"] = "config"
                    elif e["status"] == "error" and ex["status"] not in ("items", "config"):
                        ex["status"] = "error"
                        ex["error_code"] = e.get("error_code", 0)
        ep_w = sum(1 for e in merged_ep.values() if e["status"] == "items")
        ep_c = sum(1 for e in merged_ep.values() if e["status"] == "config")
        ep_m = sum(1 for e in merged_ep.values() if e["status"] == "empty")
        ep_e = sum(1 for e in merged_ep.values() if e["status"] == "error")
        ep_rows = ""
        for e in sorted(merged_ep.values(), key=lambda x: ({"items": 0, "config": 1, "empty": 2, "error": 3}.get(x["status"], 4), x["endpoint"])):
            if e["status"] == "items":
                _ic, _ct, _cs = "<span style='color:var(--pass);font-weight:700'>&#10003;</span>", f'{e["items_count"]} item{"s" if e["items_count"] != 1 else ""}', "color:var(--pass);font-weight:600"
            elif e["status"] == "config":
                _ic, _ct, _cs = "<span style='color:var(--warn);font-weight:700'>&#9881;</span>", "config/settings", "color:var(--warn);font-weight:600"
            elif e["status"] == "error":
                _ic, _ct, _cs = "<span style='color:var(--fail);font-weight:700'>&#10007;</span>", f'HTTP {e["error_code"]}', "color:var(--fail);font-weight:600"
            else:
                _ic, _ct, _cs = "<span style='color:var(--ifm-color-emphasis-300)'>&#9675;</span>", "0 items", "color:var(--ifm-color-emphasis-300)"
            ep_rows += f"<tr><td>{_ic}</td><td style='font-family:var(--ifm-font-family-monospace);font-size:12px'>{_esc(e['endpoint'])}</td><td style='{_cs}'>{_ct}</td></tr>"
        ep_content = (f"<div class='kpi-strip'><div class='kpi-card'><div class='kpi-label'>With Items</div><div class='kpi-val' style='color:var(--pass)'>{ep_w}</div></div><div class='kpi-card'><div class='kpi-label'>Config</div><div class='kpi-val' style='color:var(--warn)'>{ep_c}</div></div><div class='kpi-card'><div class='kpi-label'>Empty</div><div class='kpi-val' style='color:var(--ifm-color-emphasis-400)'>{ep_m}</div></div><div class='kpi-card'><div class='kpi-label'>Errors</div><div class='kpi-val' style='color:var(--fail)'>{ep_e}</div></div><div class='kpi-card'><div class='kpi-label'>Total</div><div class='kpi-val' style='color:var(--info)'>{len(merged_ep)}</div></div></div>" + f"<div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th style='width:30px'></th><th class='sortable'>Endpoint</th><th class='sortable'>Result</th></tr></thead><tbody>{ep_rows}</tbody></table></div><div class='pager'></div>")

    # ── All Checks Reference tab ──
    _cor: list[str] = []
    _ccr: dict[str, list[str]] = {}
    for cid, cdata in SAT_CHECKS.items():
        cat = cdata.get("category", "Other")
        if cat not in _ccr:
            _cor.append(cat)
            _ccr[cat] = []
        _ccr[cat].append(cid)
    cref_rows = ""
    _ss = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    for cat in _cor:
        for cid in sorted(_ccr[cat], key=lambda c: (_ss.get(SAT_CHECKS[c].get("severity", "low"), 3), c)):
            ck = SAT_CHECKS[cid]
            ru = ck.get("reference_url", "")
            rl = f"<a href='{_esc(ru)}' target='_blank' style='color:var(--ifm-color-primary-light);text-decoration:none;font-size:11px'>Docs</a>" if ru else ""
            etd = f"<td style='font-size:12px;white-space:nowrap'>{_esc(_get_effort(cid))}</td>" if show_effort else ""
            cref_rows += f"<tr><td style='font-family:var(--ifm-font-family-monospace);font-size:12px;white-space:nowrap'>{_esc(cid)}</td><td>{_esc(cat)}</td><td>{_sev_badge(ck.get('severity', 'low'))}</td>{etd}<td style='font-weight:600'>{_esc(ck.get('title', ''))}</td><td style='font-size:12px'>{_esc(ck.get('description', ''))}</td><td style='font-size:12px'>{_esc(ck.get('recommendation', ''))}</td><td>{rl}</td></tr>"
    etr = "<th class='sortable'>Effort</th>" if show_effort else ""
    checks_ref_content = (f"<p style='font-size:13px;color:var(--ifm-color-emphasis-400);margin-bottom:16px'>{len(SAT_CHECKS)} checks defined across all categories.</p>" + f"<div style='overflow-x:auto'><table class='sortable-table'><thead><tr><th class='sortable'>Check ID</th><th class='sortable'>Category</th><th class='sortable'>Severity</th>{etr}<th class='sortable'>Title</th><th>Description</th><th>Recommendation</th><th>Docs</th></tr></thead><tbody>{cref_rows}</tbody></table></div><div class='pager'></div>")

    # ── Definitions tab ──
    definitions_content = _build_definitions_html(show_effort)

    # ── Build tabs ──
    tabs: list[tuple[str, str, str, str]] = [
        ("summary", "Summary", f"{len(results)} workspaces", summary_content),
        ("comparison", "Comparison", "", comparison_content),
        ("common-issues", "Common Issues", "", common_issues_content),
    ]
    if prio_content:
        tabs.append(("prioritised", f"Prioritised ({len(comb_prio)})", str(len(comb_prio)), prio_content))
    if ep_content:
        tabs.append(("api-endpoints", "API Endpoints", "", ep_content))
    tabs.append(("checks-reference", f"All Checks ({len(SAT_CHECKS)})", str(len(SAT_CHECKS)), checks_ref_content))
    tabs.append(("definitions", "Definitions", "", definitions_content))

    # Per-workspace sidebar links
    ws_nav_items = ""
    for name, r in sorted(results, key=lambda x: x[0].lower()):
        ws_tid = "ws-" + _sanitize_name(name)
        sc = "var(--pass)" if r.overall_score >= 80 else ("var(--warn)" if r.overall_score >= 60 else "var(--fail)")
        ws_nav_items += f'<li class="menu__list-item"><button class="menu__link" data-tab="{ws_tid}"><span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(name)}</span><span style="color:{sc};font-weight:600;font-size:12px;flex-shrink:0;margin-left:auto">{r.overall_score}</span></button></li>\n'
        tabs.append((ws_tid, name, str(r.overall_score), ws_tab_content[name]))

    sidebar_items = ""
    tab_panels = ""
    for i, (tid, label, badge_text, content) in enumerate(tabs):
        if not tid.startswith("ws-"):
            active = " menu__link--active" if i == 0 else ""
            bh = f'<span class="menu__badge">{_esc(badge_text)}</span>' if badge_text else ""
            sidebar_items += f'<li class="menu__list-item"><button class="menu__link{active}" data-tab="{tid}"><span>{_esc(label)}</span>{bh}</button></li>\n'
        vis = " visible" if i == 0 else ""
        pn = '<nav class="pagination-nav">'
        if i > 0:
            pn += f"<a class='pagination-nav__link pagination-nav__link--prev' onclick=\"switchTab('{tabs[i-1][0]}')\"><span class='pagination-nav__sublabel'>&#8249; Previous</span><span class='pagination-nav__label'>{_esc(tabs[i-1][1])}</span></a>"
        if i < len(tabs) - 1:
            pn += f"<a class='pagination-nav__link pagination-nav__link--next' onclick=\"switchTab('{tabs[i+1][0]}')\"><span class='pagination-nav__sublabel'>Next &#8250;</span><span class='pagination-nav__label'>{_esc(tabs[i+1][1])}</span></a>"
        pn += "</nav>"
        tab_panels += f'<div class="tab-panel{vis}" id="panel-{tid}"><div class="card"><h2>{_esc(label)}</h2>{content}</div>{pn}</div>\n'

    ftl = tabs[0][1] if tabs else "Summary"

    html_doc = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SAT Scanner &mdash; Combined Summary</title>
<style>{_MODERN_CSS}</style><script>try{{var t=localStorage.getItem('satscanner-theme');if(t)document.documentElement.setAttribute('data-theme',t);}}catch(e){{}}</script></head><body>
<nav class="navbar">
  <button class="sidebar-toggle" id="sidebar-toggle-btn" onclick="toggleSidebar()" title="Hide sidebar"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg></button>
  <a class="navbar__brand" href="#"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg> SAT Scanner</a>
  <div class="navbar__items">
    <span class="navbar__link navbar__link--active">Docs</span>
    <span class="navbar__link" style="color:var(--ifm-color-emphasis-400);cursor:default">Combined Summary</span>
    <span class="navbar__version">v{__version__}</span>
    <span class="navbar__search"><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="7" cy="7" r="5"/><line x1="11" y1="11" x2="15" y2="15"/></svg>Search<kbd>&#8984;K</kbd></span>
    <button class="theme-toggle" id="theme-toggle-btn" onclick="toggleTheme()" title="Switch to light mode"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg></button>
  </div>
</nav>
<div class="layout">
  <aside class="sidebar"><ul class="menu__list">
    <li class="menu__category">Dashboard <span class="menu__category-chevron">&#9662;</span></li>
    {sidebar_items}
    <li class="menu__category" style="margin-top:16px;border-top:1px solid var(--ifm-color-emphasis-200);padding-top:12px">Workspaces <span class="menu__category-chevron">&#9662;</span></li>
    {ws_nav_items}
  </ul></aside>
  <main class="main">
    <nav class="breadcrumb"><a class="breadcrumb__home" href="#"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></a><span class="breadcrumb__sep">&#8250;</span><span class="breadcrumb__item">Combined Summary</span><span class="breadcrumb__sep">&#8250;</span><span class="breadcrumb__current" id="breadcrumb-current">{_esc(ftl)}</span></nav>
    <div class="landing-hero"><div class="landing-hero__title">Combined Summary</div><div class="landing-hero__tagline">{len(results)} Workspaces &middot; Multi-workspace security overview</div><div class="landing-hero__sub">Generated {_ts}</div></div>
    <div class="kpi-strip">
      <div class="kpi-card"><div class="kpi-label">Workspaces</div><div class="kpi-val" style="color:var(--ifm-color-primary-light)">{len(results)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Avg Score</div><div class="kpi-val" style="color:{avg_color}">{avg_score}</div><div class="kpi-sub">{_esc(avg_grade)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Total Checks</div><div class="kpi-val">{total_checks_all}</div></div>
      <div class="kpi-card"><div class="kpi-label">Passed</div><div class="kpi-val" style="color:var(--pass)">{total_passed}</div></div>
      <div class="kpi-card"><div class="kpi-label">Failed</div><div class="kpi-val" style="color:var(--fail)">{total_failed}</div></div>
      <div class="kpi-card"><div class="kpi-label">Warnings</div><div class="kpi-val" style="color:var(--warn)">{total_warnings}</div></div>
      <div class="kpi-card"><div class="kpi-label">N/A</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-400)">{total_na}</div></div>
      <div class="kpi-card"><div class="kpi-label">API Errors</div><div class="kpi-val" style="color:var(--ifm-color-emphasis-400)">{total_api_errors}</div></div>
    </div>
    <div class="search-wrap"><input type="text" id="searchInput" placeholder="Search across all tabs..." oninput="doSearch(this.value)"><span id="searchCount" class="search-count"></span></div>
    {tab_panels}
  </main>
  <aside class="toc-sidebar"><div class="toc-sidebar__title">On this page</div><ul class="toc-sidebar__list" id="toc-list"></ul></aside>
</div>
<button class="toc-toggle" id="toc-toggle-btn" onclick="toggleTOC()" title="Hide table of contents">&#10005;</button>
<footer class="footer"><div class="footer__title">SAT Scanner</div><div class="footer__links"><span class="footer__link">Modern Profile</span><span class="footer__link">v{__version__}</span></div><div class="footer__copyright">Generated by SAT Scanner</div></footer>
<script>{_MODERN_JS}</script>
</body></html>"""

    out_path = output_dir / f"sat-combined-summary-{date_str}.html"
    out_path.write_text(html_doc, encoding="utf-8")
    return None
