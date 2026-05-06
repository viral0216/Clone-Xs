"""Derive a Zerobus server endpoint from a Databricks workspace URL.

The Zerobus gRPC endpoint format is region-specific:
- AWS:   https://<workspace_id>.zerobus.<region>.cloud.databricks.com
- Azure: https://<workspace_id>.zerobus.<region>.azuredatabricks.net
- GCP:   https://<workspace_id>.zerobus.<region>.gcp.databricks.com

Browsers can't resolve DNS directly, so the UI calls this server-side
helper. We:

1. Parse the URL → detect cloud + extract workspace_id where the URL
   contains it (Azure puts it in the hostname, AWS/GCP put it in the
   ``?o=`` query parameter only after login).
2. Resolve the workspace hostname through its CNAME chain. The chain
   leaks the region:
     dbc-XXX.cloud.databricks.com
       → <friendly>.cloud.databricks.com   (e.g. ``ohio.cloud.databricks.com``)
       → <elb>.<region>.amazonaws.com      (e.g. ``…us-east-2.amazonaws.com``)
3. Build the Zerobus URL from (workspace_id, region, cloud).

Azure exposes the region in the CNAME chain too — workspace hostnames
alias through ``<region>.azuredatabricks.net`` (e.g. ``uksouth``,
``eastus2``) before terminating at ``ingress.<region>.azuredatabricks.net``.
GCP doesn't always leak the region via DNS so this helper returns a
structured failure for GCP (``cloud="gcp"``, ``region=None``) and the
UI prompts the user for it.
"""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qs, urlparse


Cloud = Literal["aws", "azure", "gcp", "unknown"]


@dataclass
class ResolvedEndpoint:
    """Result of trying to derive a Zerobus endpoint."""

    server_endpoint: str | None
    workspace_id: str | None
    region: str | None
    cloud: Cloud
    notes: list[str]
    error: str | None = None
    # Whether the resolved region is on the published Zerobus availability
    # list for its cloud. ``None`` when region detection failed or cloud=
    # ``"unknown"``. ``False`` is a strong signal the user should verify
    # with Databricks support before trying — a derived endpoint pointing
    # at an unsupported region just resolves to a generic 404 / refused
    # connection at run time.
    region_supported: bool | None = None
    # Sub-flag — set to True when the resolved region exists but is
    # documented as single-AZ rather than multi-AZ. Today only
    # ``westus`` and ``northcentralus`` on Azure. Surfaced so the UI
    # can warn that throughput / availability characteristics differ.
    region_single_az: bool = False

    def to_dict(self) -> dict:
        return {
            "server_endpoint": self.server_endpoint,
            "workspace_id": self.workspace_id,
            "region": self.region,
            "cloud": self.cloud,
            "notes": self.notes,
            "error": self.error,
            "region_supported": self.region_supported,
            "region_single_az": self.region_single_az,
        }


# Common AWS regions where Databricks runs. We use the resolved CNAME
# chain to pick the right one — this set is just the search universe.
_KNOWN_AWS_REGIONS = (
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-central-1",
    "eu-north-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-south-1",
    "ca-central-1",
    "sa-east-1",
)


# Regions where the Zerobus Ingest connector is published as available.
# Sourced from the Azure docs (zerobus-limits → Availability) and the
# AWS / GCP equivalents. Update when the docs change — this is a static
# allow-list rather than a probe because the SDK doesn't expose region
# enumeration. False positives (region IS supported but missing here)
# only produce a warning, not a hard block, so a stale list degrades
# gracefully.
_ZEROBUS_SUPPORTED_AZURE_REGIONS = frozenset(
    {
        "eastus",
        "eastus2",
        "westus",
        "westus2",
        "northcentralus",
        "southcentralus",
        "centralus",
        "westeurope",
        "northeurope",
        "uksouth",
        "australiaeast",
    }
)

# Azure regions documented as single-AZ rather than multi-AZ. Surfaced
# as a softer warning — the connector still works there but recovery
# characteristics differ from multi-AZ regions.
_ZEROBUS_AZURE_SINGLE_AZ_REGIONS = frozenset({"westus", "northcentralus"})

# AWS regions where Zerobus is documented as available. Same caveat as
# the Azure list — keep in sync with the published feature-region
# matrix.
_ZEROBUS_SUPPORTED_AWS_REGIONS = frozenset(
    {
        "us-east-1",
        "us-east-2",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
    }
)

# GCP — published list is small and changes; keep an empty set for now
# so we always emit "couldn't verify" rather than false-negative.
_ZEROBUS_SUPPORTED_GCP_REGIONS: frozenset[str] = frozenset()


def _check_region_supported(cloud: Cloud, region: str | None) -> tuple[bool | None, bool]:
    """Return ``(region_supported, single_az)`` for the given cloud+region.

    ``region_supported`` is ``None`` when we don't have an authoritative
    list for this cloud (currently GCP) or when ``region`` is missing —
    "couldn't verify" rather than "definitely unsupported".
    """
    if not region:
        return None, False
    if cloud == "azure":
        return (
            region in _ZEROBUS_SUPPORTED_AZURE_REGIONS,
            region in _ZEROBUS_AZURE_SINGLE_AZ_REGIONS,
        )
    if cloud == "aws":
        return region in _ZEROBUS_SUPPORTED_AWS_REGIONS, False
    if cloud == "gcp":
        # No published list checked into the resolver yet.
        return None, False
    return None, False


def _detect_cloud(host: str) -> Cloud:
    """Classify a Databricks workspace hostname by cloud."""
    h = host.lower()
    if h.endswith(".azuredatabricks.net"):
        return "azure"
    if h.endswith(".gcp.databricks.com"):
        return "gcp"
    if h.endswith(".cloud.databricks.com"):
        return "aws"
    return "unknown"


def _extract_workspace_id(host: str, query: str, cloud: Cloud) -> str | None:
    """Pull workspace_id from the URL where the cloud puts it.

    - Azure: in the hostname as ``adb-<wsid>.<n>.azuredatabricks.net``.
    - AWS:   only in the ``?o=<wsid>`` query param (after login).
    - GCP:   in the hostname as ``<wsid>.<n>.gcp.databricks.com``.
    """
    # ?o= takes precedence — when present it's authoritative regardless of cloud
    qs = parse_qs(query or "")
    if "o" in qs and qs["o"] and qs["o"][0].isdigit():
        return qs["o"][0]

    h = host.lower()
    if cloud == "azure":
        # adb-<digits>.<n>.azuredatabricks.net
        m = re.match(r"^adb-(\d+)\.", h)
        if m:
            return m.group(1)
    elif cloud == "gcp":
        # <digits>.<n>.gcp.databricks.com
        m = re.match(r"^(\d+)\.", h)
        if m:
            return m.group(1)
    # AWS deployment-name URLs (dbc-XXX.cloud.databricks.com) don't carry
    # the workspace_id at all; the user must include ?o=...
    return None


def _resolve_aws_region(host: str) -> tuple[str | None, list[str]]:
    """Walk the CNAME chain for `host` to find the AWS region.

    Databricks AWS workspaces resolve through a friendly CNAME (e.g.
    ``ohio.cloud.databricks.com``) into an AWS ELB hostname that
    embeds the region (``…elb.us-east-2.amazonaws.com``). Both layers
    are useful — we prefer the explicit ``<region>.amazonaws.com``
    match because it's unambiguous.

    Returns (region | None, notes_list_for_diagnostics).
    """
    notes: list[str] = []
    try:
        # Use socket.gethostbyname_ex which returns aliases (CNAMEs) +
        # the final A record's hostname/IPs. Easier than pulling in dnspython.
        canonical, aliases, ips = socket.gethostbyname_ex(host)
        chain = [host, *aliases, canonical]
        notes.append(f"DNS chain: {' → '.join(chain)}")
        notes.append(f"IPs: {', '.join(ips)}")
    except socket.gaierror as e:
        return None, [f"DNS resolution failed for {host}: {e}"]

    # Walk all hostnames we've seen for an explicit region marker.
    for name in chain:
        for region in _KNOWN_AWS_REGIONS:
            if f".{region}.amazonaws.com" in name or f".{region}.cloud.databricks.com" in name:
                return region, notes

    # Fallback: AWS friendly-name CNAME matches a region.
    # e.g. ``ohio.cloud.databricks.com`` → us-east-2.
    friendly_to_region = {
        "ohio": "us-east-2",
        "oregon": "us-west-2",
        "n-virginia": "us-east-1",
        "nvirginia": "us-east-1",
        "n-california": "us-west-1",
        "ireland": "eu-west-1",
        "london": "eu-west-2",
        "frankfurt": "eu-central-1",
        "stockholm": "eu-north-1",
        "singapore": "ap-southeast-1",
        "sydney": "ap-southeast-2",
        "tokyo": "ap-northeast-1",
        "seoul": "ap-northeast-2",
        "mumbai": "ap-south-1",
        "central": "ca-central-1",
        "saopaulo": "sa-east-1",
        "sao-paulo": "sa-east-1",
    }
    for name in chain:
        first = name.split(".")[0].lower()
        if first in friendly_to_region:
            notes.append(f"matched friendly name {first!r} → {friendly_to_region[first]}")
            return friendly_to_region[first], notes

    return None, notes


def _resolve_azure_region(host: str) -> tuple[str | None, list[str]]:
    """Walk the CNAME chain for `host` to find the Azure region.

    Azure Databricks workspaces alias through a region-named hostname
    of the form ``<region>.azuredatabricks.net`` (e.g.
    ``uksouth.azuredatabricks.net``, ``eastus2.azuredatabricks.net``)
    on the way to ``ingress.<region>.azuredatabricks.net``. Either name
    in the chain leaks the region; we accept both.

    Returns (region | None, notes_list_for_diagnostics).
    """
    notes: list[str] = []
    try:
        canonical, aliases, ips = socket.gethostbyname_ex(host)
        chain = [host, *aliases, canonical]
        notes.append(f"DNS chain: {' → '.join(chain)}")
        notes.append(f"IPs: {', '.join(ips)}")
    except socket.gaierror as e:
        return None, [f"DNS resolution failed for {host}: {e}"]

    # Match either ``<region>.azuredatabricks.net`` (the friendly
    # region alias) or ``ingress.<region>.azuredatabricks.net`` (the
    # canonical). Region tokens are lowercase alphanumerics, no dots —
    # `eastus2`, `northeurope`, `uksouth`, etc.
    region_re = re.compile(
        r"^(?:ingress\.)?([a-z0-9]+)\.azuredatabricks\.net$",
        re.IGNORECASE,
    )
    for name in chain:
        m = region_re.match(name.strip(".").lower())
        if m and m.group(1) not in {"adb", "ingress"}:
            region = m.group(1)
            notes.append(f"matched region {region!r} from CNAME {name!r}")
            return region, notes

    return None, notes


def derive_zerobus_endpoint(workspace_url: str) -> ResolvedEndpoint:
    """Derive a Zerobus server endpoint from a Databricks workspace URL.

    The URL can be:
      - The bare workspace URL: ``https://dbc-….cloud.databricks.com``
      - A logged-in URL with ``?o=<workspace_id>`` query param
      - An Azure URL: ``https://adb-<wsid>.<n>.azuredatabricks.net``

    Returns a ``ResolvedEndpoint`` with whatever could be derived plus
    diagnostic notes. ``error`` is set when the URL is unparseable or
    a required piece (workspace_id / region) couldn't be determined.
    """
    raw = (workspace_url or "").strip()
    if not raw:
        return ResolvedEndpoint(
            None,
            None,
            None,
            "unknown",
            [],
            error="workspace_url is empty",
        )
    # Add scheme if user pasted a bare hostname.
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if not host:
        return ResolvedEndpoint(
            None,
            None,
            None,
            "unknown",
            [],
            error=f"could not parse hostname from {workspace_url!r}",
        )

    cloud = _detect_cloud(host)
    notes: list[str] = [f"detected cloud: {cloud}", f"hostname: {host}"]
    workspace_id = _extract_workspace_id(host, parsed.query, cloud)

    if cloud == "unknown":
        return ResolvedEndpoint(
            None,
            workspace_id,
            None,
            cloud,
            notes,
            error=(
                "URL doesn't match the AWS / Azure / GCP Databricks workspace "
                "patterns — paste a URL like https://dbc-…cloud.databricks.com, "
                "https://adb-…azuredatabricks.net, or https://….gcp.databricks.com"
            ),
        )

    if not workspace_id:
        return ResolvedEndpoint(
            None,
            None,
            None,
            cloud,
            notes,
            error=(
                "Workspace ID not found. AWS workspace URLs only show it after "
                "login as the ?o=<digits> query param — open any page in your "
                "workspace, copy the URL from the address bar, and paste that "
                "(it'll have ?o=… appended)."
            ),
        )

    # Region detection per cloud.
    region: str | None = None
    if cloud == "aws":
        region, region_notes = _resolve_aws_region(host)
        notes.extend(region_notes)
        endpoint_tld = "cloud.databricks.com"
    elif cloud == "gcp":
        # GCP region detection via DNS is patchy — defer to the user.
        endpoint_tld = "gcp.databricks.com"
    else:  # azure
        # Azure DOES leak the region via the CNAME chain (workspace
        # hostnames alias through `<region>.azuredatabricks.net`).
        region, region_notes = _resolve_azure_region(host)
        notes.extend(region_notes)
        endpoint_tld = "azuredatabricks.net"

    if not region:
        return ResolvedEndpoint(
            None,
            workspace_id,
            None,
            cloud,
            notes,
            error=(
                f"Workspace ID resolved to {workspace_id} but the {cloud.upper()} "
                f"region couldn't be determined automatically. Find it in the "
                f"{'Azure Portal → workspace → Location' if cloud == 'azure' else 'Account Console → Workspaces → Region'} "
                f"and pass it explicitly. Endpoint format: "
                f"https://{workspace_id}.zerobus.<region>.{endpoint_tld}"
            ),
        )

    server_endpoint = f"https://{workspace_id}.zerobus.{region}.{endpoint_tld}"
    notes.append(f"resolved endpoint: {server_endpoint}")
    region_supported, region_single_az = _check_region_supported(cloud, region)
    if region_supported is False:
        notes.append(
            f"warning: region {region!r} is not on the published Zerobus "
            f"availability list for {cloud.upper()} — connection may fail"
        )
    elif region_single_az:
        notes.append(
            f"warning: region {region!r} is documented as single-AZ on Azure; "
            f"availability characteristics differ from multi-AZ regions"
        )
    return ResolvedEndpoint(
        server_endpoint=server_endpoint,
        workspace_id=workspace_id,
        region=region,
        cloud=cloud,
        notes=notes,
        region_supported=region_supported,
        region_single_az=region_single_az,
    )
