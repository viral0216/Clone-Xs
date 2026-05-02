"""Tests for src/zerobus_endpoint_resolver.py and the derive-endpoint API."""

from unittest.mock import patch

import pytest

from src.zerobus_endpoint_resolver import (
    _detect_cloud,
    _extract_workspace_id,
    derive_zerobus_endpoint,
)


class TestDetectCloud:
    @pytest.mark.parametrize(
        "host,expected",
        [
            ("dbc-15d1cbfa-8d5c.cloud.databricks.com", "aws"),
            ("DBC-FOO.CLOUD.DATABRICKS.COM", "aws"),
            ("adb-1134642475632994.14.azuredatabricks.net", "azure"),
            ("1234567890.5.gcp.databricks.com", "gcp"),
            ("example.com", "unknown"),
            ("notdatabricks.io", "unknown"),
        ],
    )
    def test_classification(self, host, expected):
        assert _detect_cloud(host) == expected


class TestExtractWorkspaceId:
    def test_aws_with_o_query_param(self):
        wsid = _extract_workspace_id(
            "dbc-15d1cbfa-8d5c.cloud.databricks.com",
            "o=2218772291954179",
            "aws",
        )
        assert wsid == "2218772291954179"

    def test_aws_without_query_returns_none(self):
        # Bare AWS deployment URL doesn't carry the workspace ID.
        wsid = _extract_workspace_id(
            "dbc-15d1cbfa-8d5c.cloud.databricks.com",
            "",
            "aws",
        )
        assert wsid is None

    def test_azure_extracted_from_hostname(self):
        wsid = _extract_workspace_id(
            "adb-1134642475632994.14.azuredatabricks.net",
            "",
            "azure",
        )
        assert wsid == "1134642475632994"

    def test_gcp_extracted_from_hostname(self):
        wsid = _extract_workspace_id(
            "1234567890123456.5.gcp.databricks.com",
            "",
            "gcp",
        )
        assert wsid == "1234567890123456"

    def test_query_param_overrides_hostname(self):
        # If both ?o= and the hostname carry a wsid, ?o= wins (it's
        # authoritative for whoever the user is logged in as).
        wsid = _extract_workspace_id(
            "adb-1111111111111111.14.azuredatabricks.net",
            "o=9999999999999999",
            "azure",
        )
        assert wsid == "9999999999999999"

    def test_non_numeric_o_param_ignored(self):
        wsid = _extract_workspace_id(
            "dbc-X.cloud.databricks.com",
            "o=not-a-number",
            "aws",
        )
        assert wsid is None


class TestDeriveEndpoint:
    def test_aws_full_url_with_dns_resolution(self):
        # Patch DNS to return a deterministic CNAME chain that points at us-east-2.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "public-ingress-x.elb.us-east-2.amazonaws.com",
                ["ohio.cloud.databricks.com"],
                ["3.128.237.222"],
            ),
        ):
            r = derive_zerobus_endpoint(
                "https://dbc-15d1cbfa-8d5c.cloud.databricks.com/?o=2218772291954179"
            )
        assert r.error is None
        assert r.cloud == "aws"
        assert r.workspace_id == "2218772291954179"
        assert r.region == "us-east-2"
        assert (
            r.server_endpoint == "https://2218772291954179.zerobus.us-east-2.cloud.databricks.com"
        )

    def test_aws_falls_back_to_friendly_name_when_no_explicit_region(self):
        # Some CNAME chains don't include the AWS ELB hostname — only
        # the friendly name. Confirm the friendly-name lookup table works.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "ohio.cloud.databricks.com",
                [],
                ["3.128.237.222"],
            ),
        ):
            r = derive_zerobus_endpoint("https://dbc-foo.cloud.databricks.com/?o=1234567890")
        assert r.region == "us-east-2"

    def test_aws_bare_url_missing_workspace_id_returns_helpful_error(self):
        r = derive_zerobus_endpoint("https://dbc-15d1cbfa-8d5c.cloud.databricks.com")
        assert r.error is not None
        assert "?o=" in r.error
        assert r.server_endpoint is None
        assert r.cloud == "aws"

    def test_azure_url_resolves_region_via_dns_chain(self):
        # Azure workspaces alias through `<region>.azuredatabricks.net`
        # (e.g. uksouth.azuredatabricks.net) before terminating at
        # `ingress.<region>.azuredatabricks.net`. The resolver walks
        # this chain to extract the region, same shape as the AWS path.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "ingress.uksouth.azuredatabricks.net",
                [
                    "adb-1134642475632994.14.azuredatabricks.net",
                    "uksouth.azuredatabricks.net",
                ],
                ["4.158.9.160"],
            ),
        ):
            r = derive_zerobus_endpoint("https://adb-1134642475632994.14.azuredatabricks.net")
        assert r.error is None
        assert r.cloud == "azure"
        assert r.workspace_id == "1134642475632994"
        assert r.region == "uksouth"
        assert r.server_endpoint == "https://1134642475632994.zerobus.uksouth.azuredatabricks.net"

    def test_azure_url_falls_back_when_dns_doesnt_expose_region(self):
        # Defensive: if a future Azure DNS topology stops aliasing
        # through `<region>.azuredatabricks.net` (or the resolver runs
        # somewhere DNS is sandboxed), the helper still returns the
        # workspace_id and a structured error pointing the user at
        # the Azure Portal.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "some-private-host.internal",
                [],
                ["10.0.0.1"],
            ),
        ):
            r = derive_zerobus_endpoint("https://adb-1134642475632994.14.azuredatabricks.net")
        assert r.cloud == "azure"
        assert r.workspace_id == "1134642475632994"
        assert r.region is None
        assert r.error is not None
        assert "azuredatabricks.net" in r.error  # hint about endpoint format

    def test_unknown_url_pattern_rejected_with_clear_message(self):
        r = derive_zerobus_endpoint("https://example.com/foo")
        assert r.cloud == "unknown"
        assert r.error is not None
        assert "azuredatabricks" in r.error  # mentions all three clouds in the hint

    def test_dns_failure_surfaced_in_error(self):
        import socket as _socket

        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            side_effect=_socket.gaierror("Name resolution failed"),
        ):
            r = derive_zerobus_endpoint("https://dbc-foo.cloud.databricks.com/?o=1234567890")
        assert r.region is None
        assert r.error is not None
        # Notes carry the DNS-failure detail for diagnostics.
        assert any("DNS resolution failed" in n for n in r.notes)

    def test_empty_url_rejected(self):
        r = derive_zerobus_endpoint("")
        assert r.error == "workspace_url is empty"

    def test_bare_hostname_gets_https_prefix(self):
        # Convenience: pasting just `dbc-foo.cloud.databricks.com` works
        # too, no scheme required.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=("ohio.cloud.databricks.com", [], ["1.2.3.4"]),
        ):
            r = derive_zerobus_endpoint(
                "dbc-15d1cbfa-8d5c.cloud.databricks.com/?o=2218772291954179"
            )
        assert r.error is None
        assert r.server_endpoint and "us-east-2" in r.server_endpoint


# ----------------- API endpoint round-trip -----------------


class TestDeriveEndpointRoute:
    def test_round_trip_returns_resolved_dict(self, client):
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "public-ingress-x.elb.us-east-2.amazonaws.com",
                ["ohio.cloud.databricks.com"],
                ["3.128.237.222"],
            ),
        ):
            r = client.post(
                "/api/generate/demo-data/zerobus/derive-endpoint",
                json={"workspace_url": "https://dbc-foo.cloud.databricks.com/?o=2218772291954179"},
            )
        assert r.status_code == 200
        body = r.json()
        assert (
            body["server_endpoint"]
            == "https://2218772291954179.zerobus.us-east-2.cloud.databricks.com"
        )
        assert body["cloud"] == "aws"
        assert body["region"] == "us-east-2"
        assert body["error"] is None

    def test_unparseable_url_returns_200_with_error(self, client):
        # Failure path is 200 + structured error so the UI can render
        # a friendly message rather than dealing with HTTP status codes.
        r = client.post(
            "/api/generate/demo-data/zerobus/derive-endpoint",
            json={"workspace_url": "https://example.com"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["server_endpoint"] is None
        assert body["error"] is not None
