# Third-Party Licenses

Clone-Xs is distributed under the [MIT License](LICENSE). This file acknowledges the open-source libraries used by Clone-Xs and their respective licenses.

All dependencies use permissive licenses (MIT, Apache-2.0, BSD, ISC) that are fully compatible with MIT distribution.

---

## Python Dependencies

| Package | License | Usage |
|---------|---------|-------|
| [databricks-sdk](https://github.com/databricks/databricks-sdk-py) | Apache-2.0 | Databricks workspace and Unity Catalog API client |
| [pyyaml](https://github.com/yaml/pyyaml) | MIT | YAML configuration file parsing |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | MIT | Environment variable loading from `.env` files |
| [fastapi](https://github.com/fastapi/fastapi) | MIT | REST API framework |
| [uvicorn](https://github.com/encode/uvicorn) | BSD-3-Clause | ASGI server for FastAPI |
| [python-multipart](https://github.com/Kludex/python-multipart) | Apache-2.0 | Multipart form data parsing for FastAPI |
| [click](https://github.com/pallets/click) | BSD-3-Clause | CLI framework (via Databricks SDK) |
| [pydantic](https://github.com/pydantic/pydantic) | MIT | Data validation (via FastAPI) |
| [pytest](https://github.com/pytest-dev/pytest) | MIT | Testing framework (dev only) |
| [ruff](https://github.com/astral-sh/ruff) | MIT | Python linter (dev only) |
| [presidio-analyzer](https://github.com/microsoft/presidio) | MIT | NLP-based PII detection (optional) |
| [presidio-anonymizer](https://github.com/microsoft/presidio) | MIT | PII anonymization (optional) |

## Frontend Dependencies

| Package | License | Usage |
|---------|---------|-------|
| [react](https://github.com/facebook/react) | MIT | UI framework |
| [react-dom](https://github.com/facebook/react) | MIT | React DOM rendering |
| [react-router-dom](https://github.com/remix-run/react-router) | MIT | Client-side routing |
| [vite](https://github.com/vitejs/vite) | MIT | Build tool and dev server |
| [typescript](https://github.com/microsoft/TypeScript) | Apache-2.0 | Type system |
| [tailwindcss](https://github.com/tailwindlabs/tailwindcss) | MIT | Utility-first CSS framework |
| [@tanstack/react-query](https://github.com/TanStack/query) | MIT | Server state management |
| [recharts](https://github.com/recharts/recharts) | MIT | Charting library |
| [lucide-react](https://github.com/lucide-icons/lucide) | ISC | Icon library |
| [class-variance-authority](https://github.com/joe-bell/cva) | Apache-2.0 | Component variant utility |
| [clsx](https://github.com/lukeed/clsx) | MIT | Class name utility |
| [tailwind-merge](https://github.com/dcastil/tailwind-merge) | MIT | Tailwind class merging |
| [sonner](https://github.com/emilkowalski/sonner) | MIT | Toast notifications |
| [shadcn/ui](https://github.com/shadcn-ui/ui) | MIT | UI component primitives |
| [@dnd-kit](https://github.com/clauderic/dnd-kit) | MIT | Drag and drop |
| [@base-ui/react](https://github.com/mui/base-ui) | MIT | Unstyled UI primitives |
| [@uiw/react-textarea-code-editor](https://github.com/uiwjs/react-textarea-code-editor) | MIT | Code editor component |
| [@axe-core/react](https://github.com/dequelabs/axe-core) | MPL-2.0 | Accessibility testing (dev only, not distributed) |

## Desktop Dependencies

| Package | License | Usage |
|---------|---------|-------|
| [electron](https://github.com/electron/electron) | MIT | Desktop app framework |
| [electron-builder](https://github.com/electron-userland/electron-builder) | MIT | Desktop app packaging |

## Documentation Dependencies

| Package | License | Usage |
|---------|---------|-------|
| [@docusaurus/core](https://github.com/facebook/docusaurus) | MIT | Documentation site framework |
| [@docusaurus/preset-classic](https://github.com/facebook/docusaurus) | MIT | Docusaurus preset |
| [@mdx-js/react](https://github.com/mdx-js/mdx) | MIT | MDX rendering |
| [prism-react-renderer](https://github.com/FormidableLabs/prism-react-renderer) | MIT | Code syntax highlighting |

---

## Apache-2.0 Notice

The following dependencies are licensed under the Apache License, Version 2.0. In compliance with the Apache-2.0 license terms, this notice is provided:

**Databricks SDK for Python**
Copyright Databricks, Inc.
Licensed under the Apache License, Version 2.0.
https://github.com/databricks/databricks-sdk-py/blob/main/LICENSE

**python-multipart**
Licensed under the Apache License, Version 2.0.
https://github.com/Kludex/python-multipart/blob/master/LICENSE.md

**TypeScript**
Copyright Microsoft Corporation.
Licensed under the Apache License, Version 2.0.
https://github.com/microsoft/TypeScript/blob/main/LICENSE.txt

**class-variance-authority**
Licensed under the Apache License, Version 2.0.
https://github.com/joe-bell/cva/blob/main/LICENSE

You may obtain a copy of the Apache License at: https://www.apache.org/licenses/LICENSE-2.0

---

## Summary

| License | Count | Distributed? |
|---------|-------|-------------|
| MIT | ~32 | Yes |
| Apache-2.0 | 4 | Yes |
| BSD-3-Clause | 2 | Yes |
| ISC | 1 | Yes |
| MPL-2.0 | 1 | No (dev only) |

All runtime dependencies use permissive open-source licenses fully compatible with the MIT License.
