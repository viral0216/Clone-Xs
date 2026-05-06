"""Tests for src/zerobus_endpoint_resolver.py and the derive-endpoint API."""

from unittest.mock import patch

import pytest

from src.zerobus_endpoint_resolver import (
    _check_region_supported,
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


# ----------------- Region whitelist -----------------


class TestCheckRegionSupported:
    @pytest.mark.parametrize(
        "region",
        ["eastus", "eastus2", "westus2", "westeurope", "uksouth", "australiaeast"],
    )
    def test_known_azure_region_is_supported_and_multi_az(self, region):
        supported, single_az = _check_region_supported("azure", region)
        assert supported is True
        assert single_az is False

    @pytest.mark.parametrize("region", ["westus", "northcentralus"])
    def test_known_azure_single_az_region_flagged(self, region):
        # westus / northcentralus are documented as single-AZ — supported
        # is True (the connector works), but single_az flag is True so
        # the UI can warn about the differing availability profile.
        supported, single_az = _check_region_supported("azure", region)
        assert supported is True
        assert single_az is True

    def test_unknown_azure_region_not_supported(self):
        # A real Azure region (japaneast) that isn't on the Zerobus list
        # should resolve to False — the warning surface in the UI keys
        # off this so this case must stay distinct from the None
        # ("can't verify") case.
        supported, single_az = _check_region_supported("azure", "japaneast")
        assert supported is False
        assert single_az is False

    @pytest.mark.parametrize("region", ["us-east-1", "us-east-2", "us-west-2", "eu-west-1"])
    def test_known_aws_region_is_supported(self, region):
        supported, single_az = _check_region_supported("aws", region)
        assert supported is True
        assert single_az is False

    def test_unknown_aws_region_not_supported(self):
        supported, _single_az = _check_region_supported("aws", "us-west-1")
        assert supported is False

    def test_gcp_region_returns_none_not_false(self):
        # GCP has no published list checked into the resolver, so we
        # report "couldn't verify" rather than falsely flagging the
        # region as unsupported.
        supported, single_az = _check_region_supported("gcp", "us-central1")
        assert supported is None
        assert single_az is False

    def test_missing_region_returns_none(self):
        supported, single_az = _check_region_supported("azure", None)
        assert supported is None
        assert single_az is False


class TestDeriveEndpointRegionFlags:
    def test_known_supported_aws_region_sets_flag_true(self):
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
        assert r.region == "us-east-2"
        assert r.region_supported is True
        assert r.region_single_az is False

    def test_single_az_azure_region_emits_warning_note(self):
        # westus is supported but single-AZ — note string must mention
        # "single-AZ" so the UI can match on it for the softer warning.
        with patch(
            "src.zerobus_endpoint_resolver.socket.gethostbyname_ex",
            return_value=(
                "ingress.westus.azuredatabricks.net",
                ["adb-1134642475632994.14.azuredatabricks.net", "westus.azuredatabricks.net"],
                ["1.2.3.4"],
            ),
        ):
            r = derive_zerobus_endpoint("https://adb-1134642475632994.14.azuredatabricks.net")
        assert r.region == "westus"
        assert r.region_supported is True
        assert r.region_single_az is True
        assert any("single-AZ" in n for n in r.notes)


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
