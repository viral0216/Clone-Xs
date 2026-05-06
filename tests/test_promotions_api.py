"""Tests for the /api/promotions REST surface."""


class TestListPlans:
    def test_returns_built_in_plans(self, client):
        r = client.get("/api/promotions/plans")
        assert r.status_code == 200
        body = r.json()
        keys = {p["key"] for p in body}
        assert "prod-to-staging" in keys
        assert "prod-to-staging-to-dev" in keys


class TestGetPlan:
    def test_returns_plan_detail(self, client):
        r = client.get("/api/promotions/plans/prod-to-staging-to-dev")
        assert r.status_code == 200
        body = r.json()
        assert len(body["steps"]) == 2

    def test_404_for_unknown(self, client):
        r = client.get("/api/promotions/plans/does-not-exist")
        assert r.status_code == 404


class TestRunPlan:
    def test_404_for_unknown_plan(self, client):
        r = client.post(
            "/api/promotions/plans/nope/run",
            json={"prefix": "pp", "warehouse_id": "wh"},
        )
        assert r.status_code == 404

    def test_run_submits_first_hop_only(self, client):
        r = client.post(
            "/api/promotions/plans/prod-to-staging-to-dev/run",
            json={"prefix": "supplier", "warehouse_id": "wh-1", "max_workers": 2},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["plan_key"] == "prod-to-staging-to-dev"
        assert body["prefix"] == "supplier"
        assert len(body["hops"]) == 2

        # First hop submitted with a job_id; rest pending
        assert body["hops"][0]["job_id"] is not None
        assert body["hops"][0]["status"] == "submitted"
        assert body["hops"][0]["source_catalog"] == "supplier_prod"
        assert body["hops"][0]["dest_catalog"] == "supplier_staging"

        assert body["hops"][1]["job_id"] is None
        assert body["hops"][1]["status"] == "pending_prior_hop"
        assert body["hops"][1]["source_catalog"] == "supplier_staging"
        assert body["hops"][1]["dest_catalog"] == "supplier_dev"
