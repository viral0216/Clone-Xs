---
sidebar_position: 16
---

# Create Databricks Job

Create persistent Databricks Jobs that run Clone-Xs on a schedule. The job appears in the Databricks Jobs UI and can be run manually, triggered, or scheduled — no CLI needed after setup.

## How It Works

1. **Upload wheel** to a UC Volume
2. **Create a clone notebook** in the workspace
3. **Create a Databricks Job** with the notebook as a task
4. Optionally set a **cron schedule**, **email notifications**, and **retries**

## From the Web UI

Navigate to **Operations > Create Job** in the sidebar.

### Job Configuration
- Select **source** and **destination** catalogs
- Choose a **UC Volume** for wheel upload
- Optionally enter a custom **job name**

### Clone Options
All standard clone options are available:
- Clone type (DEEP / SHALLOW), load type (FULL / INCREMENTAL)
- Copy options (permissions, ownership, tags, properties, security, constraints, comments)
- Performance settings (workers, parallel tables, max queries)
- Filtering (include/exclude schemas, table regex)
- Time travel (as-of timestamp or version)

### Schedule & Notifications
- Pick a **cron preset** (Daily 6 AM, Hourly, Weekdays 8 AM, etc.) or enter a custom Quartz cron expression
- Set the **timezone**
- Add **notification emails** for success/failure alerts
- Configure **retries** and **timeout**

Click **Create Databricks Job** to deploy. You'll get a direct link to the job in Databricks.

### Update Existing Job

Enter a **Job ID** in the "Update Existing Job ID" field to modify an existing job's settings without creating a new one.

## From the CLI

### Create a scheduled job

```bash
clone-catalog create-job \
  --source edp_dev \
  --dest edp_dev_00 \
  --volume /Volumes/edp_dev/packages/wheels \
  --schedule "0 0 6 * * ?" \
  --timezone "America/New_York" \
  --notification-email team@company.com \
  --tag env=production
```

### Create a job without schedule (manual trigger)

```bash
clone-catalog create-job \
  --source edp_dev \
  --dest edp_dev_00 \
  --volume /Volumes/edp_dev/packages/wheels
```

### Update an existing job

```bash
clone-catalog create-job \
  --update-job-id 12345 \
  --source edp_dev \
  --dest edp_dev_00 \
  --volume /Volumes/edp_dev/packages/wheels \
  --schedule "0 0 12 * * ?"
```

### CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `--source` | Source catalog name | Required |
| `--dest` | Destination catalog name | Required |
| `--volume` | UC Volume path for wheel upload | Interactive picker |
| `--job-name` | Custom job name | `Clone-Xs: source -> dest` |
| `--schedule` | Quartz cron expression | None (manual) |
| `--timezone` | Schedule timezone | UTC |
| `--notification-email` | Comma-separated emails | None |
| `--max-retries` | Retries on failure | 0 |
| `--timeout` | Job timeout in seconds | 7200 |
| `--tag` | Job tag as `key=value` (repeatable) | `created_by=clone-xs` |
| `--update-job-id` | Update existing job instead of creating | None |

## API Endpoint

```
POST /api/generate/create-job
```

Request body includes all clone configuration fields plus job-specific fields (schedule, notifications, tags). See the API docs at `http://localhost:8000/docs` for the full schema.
