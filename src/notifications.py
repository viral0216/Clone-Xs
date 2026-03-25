import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def _build_summary_text(summary: dict, config: dict) -> dict:
    """Build common summary data for notifications."""
    source = config["source_catalog"]
    dest = config["destination_catalog"]
    dry_run = config.get("dry_run", False)

    total_success = sum(summary[t]["success"] for t in ("tables", "views", "functions", "volumes"))
    total_failed = sum(summary[t]["failed"] for t in ("tables", "views", "functions", "volumes"))
    total_skipped = sum(summary[t]["skipped"] for t in ("tables", "views", "functions", "volumes"))

    return {
        "source": source,
        "dest": dest,
        "dry_run": dry_run,
        "total_success": total_success,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "status": "SUCCESS" if total_failed == 0 else "FAILED",
        "mode": " [DRY RUN]" if dry_run else "",
    }


def send_slack_notification(webhook_url: str, summary: dict, config: dict) -> bool:
    """Send clone summary to a Slack channel via webhook."""
    info = _build_summary_text(summary, config)

    status_emoji = ":white_check_mark:" if info["total_failed"] == 0 else ":x:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{status_emoji} Unity Catalog Clone{info['mode']}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Source:*\n`{info['source']}`"},
                {"type": "mrkdwn", "text": f"*Destination:*\n`{info['dest']}`"},
                {"type": "mrkdwn", "text": f"*Clone Type:*\n{config['clone_type']}"},
                {"type": "mrkdwn", "text": f"*Load Type:*\n{config['load_type']}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Success:*\n{info['total_success']}"},
                {"type": "mrkdwn", "text": f"*Failed:*\n{info['total_failed']}"},
                {"type": "mrkdwn", "text": f"*Skipped:*\n{info['total_skipped']}"},
                {"type": "mrkdwn", "text": f"*Schemas:*\n{summary['schemas_processed']}"},
            ],
        },
    ]

    # Add duration if available
    duration = summary.get("duration_seconds")
    if duration:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Duration:* {_format_duration(duration)}"},
        })

    errors = summary.get("errors", [])
    if errors:
        error_text = "\n".join(f"- {e}" for e in errors[:5])
        if len(errors) > 5:
            error_text += f"\n... and {len(errors) - 5} more"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Errors:*\n{error_text}"},
        })

    breakdown_lines = []
    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = summary[obj_type]
        breakdown_lines.append(
            f"  {obj_type.capitalize():12s}: "
            f"{stats['success']} ok / {stats['failed']} fail / {stats['skipped']} skip"
        )
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*Breakdown:*\n```{chr(10).join(breakdown_lines)}```"},
    })

    payload = json.dumps({"blocks": blocks}).encode("utf-8")

    try:
        req = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
        urlopen(req)
        logger.info("Slack notification sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")
        return False


def send_webhook_notification(
    webhook_url: str,
    summary: dict,
    config: dict,
    headers: dict | None = None,
    method: str = "POST",
) -> bool:
    """Send clone summary to a generic webhook (Teams, PagerDuty, custom).

    Sends a JSON payload with the clone summary.
    """
    info = _build_summary_text(summary, config)

    payload = {
        "event": "clone_catalog_completed",
        "status": info["status"],
        "source_catalog": info["source"],
        "destination_catalog": info["dest"],
        "clone_type": config["clone_type"],
        "load_type": config["load_type"],
        "dry_run": info["dry_run"],
        "schemas_processed": summary["schemas_processed"],
        "total_success": info["total_success"],
        "total_failed": info["total_failed"],
        "total_skipped": info["total_skipped"],
        "duration_seconds": summary.get("duration_seconds"),
        "breakdown": {
            obj_type: summary[obj_type]
            for obj_type in ("tables", "views", "functions", "volumes")
        },
        "errors": summary.get("errors", []),
    }

    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)

    data = json.dumps(payload).encode("utf-8")

    try:
        req = Request(webhook_url, data=data, headers=request_headers, method=method)
        urlopen(req)
        logger.info(f"Webhook notification sent to {webhook_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to send webhook notification: {e}")
        return False


def send_teams_notification(webhook_url: str, summary: dict, config: dict) -> bool:
    """Send clone summary to Microsoft Teams via incoming webhook."""
    info = _build_summary_text(summary, config)

    "00FF00" if info["total_failed"] == 0 else "FF0000"

    # Teams Adaptive Card format
    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"Unity Catalog Clone {info['status']}{info['mode']}",
                            "weight": "Bolder",
                            "size": "Large",
                            "color": "Good" if info["total_failed"] == 0 else "Attention",
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Source", "value": info["source"]},
                                {"title": "Destination", "value": info["dest"]},
                                {"title": "Clone Type", "value": config["clone_type"]},
                                {"title": "Success", "value": str(info["total_success"])},
                                {"title": "Failed", "value": str(info["total_failed"])},
                                {"title": "Skipped", "value": str(info["total_skipped"])},
                            ],
                        },
                    ],
                },
            }
        ],
    }

    data = json.dumps(card).encode("utf-8")

    try:
        req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urlopen(req)
        logger.info("Teams notification sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send Teams notification: {e}")
        return False


def send_email_notification(
    smtp_host: str,
    smtp_port: int,
    sender: str,
    recipients: list[str],
    summary: dict,
    config: dict,
    smtp_user: str | None = None,
    smtp_password: str | None = None,
    use_tls: bool = True,
) -> bool:
    """Send clone summary via email."""
    info = _build_summary_text(summary, config)

    subject = f"Unity Catalog Clone {info['status']}{info['mode']}: {info['source']} -> {info['dest']}"

    body_lines = [
        f"Unity Catalog Clone Report{info['mode']}",
        f"{'=' * 50}",
        f"Source:      {info['source']}",
        f"Destination: {info['dest']}",
        f"Clone Type:  {config['clone_type']}",
        f"Load Type:   {config['load_type']}",
        f"Schemas:     {summary['schemas_processed']}",
    ]

    duration = summary.get("duration_seconds")
    if duration:
        body_lines.append(f"Duration:    {_format_duration(duration)}")

    body_lines.extend(["", "Summary:", f"{'-' * 50}"])

    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = summary[obj_type]
        body_lines.append(
            f"  {obj_type.capitalize():12s}: "
            f"{stats['success']} success, {stats['failed']} failed, {stats['skipped']} skipped"
        )

    body_lines.extend([
        "",
        f"Total: {info['total_success']} success, {info['total_failed']} failed, {info['total_skipped']} skipped",
    ])

    errors = summary.get("errors", [])
    if errors:
        body_lines.extend(["", "Errors:", f"{'-' * 50}"])
        for err in errors:
            body_lines.append(f"  - {err}")

    body = "\n".join(body_lines)

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        if use_tls:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)

        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)

        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info(f"Email notification sent to {', '.join(recipients)}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
        return False


def _format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s}s"
    h, m = divmod(m, 60)
    return f"{h}h{m}m"
