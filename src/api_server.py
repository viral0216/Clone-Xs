"""REST API server for clone-catalog operations."""

import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

# Module-level state shared with the handler
_server_state = {
    "config": {},
    "client": None,
    "queue": None,
    "api_key": None,
}


class CloneAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Clone-Xs API."""

    def log_message(self, format, *args):
        logger.debug(f"API: {format % args}")

    def _authenticate(self) -> bool:
        """Check API key if configured."""
        api_key = _server_state.get("api_key")
        if not api_key:
            return True  # No auth required

        auth_header = self.headers.get("X-API-Key") or ""
        bearer = self.headers.get("Authorization", "").replace("Bearer ", "")
        return auth_header == api_key or bearer == api_key

    def _send_json(self, status: int, data: dict):
        """Send JSON response."""
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        """Read and parse JSON request body."""
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8"))

    def _route(self, method: str):
        """Route request to handler."""
        if not self._authenticate():
            self._send_json(401, {"error": "Unauthorized"})
            return

        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        routes = {
            ("GET", "/api/health"): self._handle_health,
            ("GET", "/api/clone"): self._handle_list_clones,
            ("POST", "/api/clone"): self._handle_submit_clone,
            ("POST", "/api/diff"): self._handle_diff,
            ("POST", "/api/validate"): self._handle_validate,
            ("GET", "/api/metrics"): self._handle_metrics,
        }

        # Check exact routes
        handler = routes.get((method, path))
        if handler:
            try:
                handler(params)
            except Exception as e:
                logger.error(f"API error: {e}")
                self._send_json(500, {"error": str(e)})
            return

        # Check parameterized routes
        if method == "GET" and path.startswith("/api/clone/"):
            job_id = path.split("/")[-1]
            try:
                self._handle_get_clone(job_id)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        if method == "DELETE" and path.startswith("/api/clone/"):
            job_id = path.split("/")[-1]
            try:
                self._handle_cancel_clone(job_id)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        self._send_json(404, {"error": "Not found"})

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def do_DELETE(self):
        self._route("DELETE")

    def _handle_health(self, params):
        self._send_json(200, {
            "status": "healthy",
            "version": "0.4.0",
        })

    def _handle_list_clones(self, params):
        queue = _server_state.get("queue")
        if not queue:
            self._send_json(200, {"jobs": []})
            return
        status_filter = params.get("status", [None])[0]
        jobs = queue.list_jobs(status=status_filter)
        self._send_json(200, {"jobs": jobs})

    def _handle_submit_clone(self, params):
        body = self._read_body()
        queue = _server_state.get("queue")
        if not queue:
            self._send_json(503, {"error": "Clone queue not initialized"})
            return

        # Build config from request body merged with server config
        config = {**_server_state.get("config", {})}
        config.update({
            "source_catalog": body.get("source_catalog", config.get("source_catalog")),
            "destination_catalog": body.get("destination_catalog", config.get("destination_catalog")),
            "clone_type": body.get("clone_type", config.get("clone_type", "DEEP")),
            "dry_run": body.get("dry_run", False),
        })

        priority = body.get("priority", 3)
        submitted_by = body.get("submitted_by", "api")

        job_id = queue.submit(
            config["source_catalog"], config["destination_catalog"],
            config, priority=priority, submitted_by=submitted_by,
        )
        self._send_json(202, {"job_id": job_id, "status": "queued"})

    def _handle_get_clone(self, job_id: str):
        queue = _server_state.get("queue")
        if not queue:
            self._send_json(503, {"error": "Queue not initialized"})
            return
        status = queue.get_status(job_id)
        if status:
            self._send_json(200, status)
        else:
            self._send_json(404, {"error": f"Job {job_id} not found"})

    def _handle_cancel_clone(self, job_id: str):
        queue = _server_state.get("queue")
        if not queue:
            self._send_json(503, {"error": "Queue not initialized"})
            return
        success = queue.cancel(job_id)
        if success:
            self._send_json(200, {"status": "cancelled", "job_id": job_id})
        else:
            self._send_json(404, {"error": f"Job {job_id} not found or not cancellable"})

    def _handle_diff(self, params):
        body = self._read_body()
        from src.diff import compare_catalogs
        client = _server_state.get("client")
        config = _server_state.get("config", {})
        warehouse_id = config.get("sql_warehouse_id", "")

        source = body.get("source_catalog", config.get("source_catalog"))
        dest = body.get("destination_catalog", config.get("destination_catalog"))
        exclude = config.get("exclude_schemas", ["information_schema", "default"])

        result = compare_catalogs(client, warehouse_id, source, dest, exclude)
        self._send_json(200, result)

    def _handle_validate(self, params):
        body = self._read_body()
        from src.validation import validate_catalog
        client = _server_state.get("client")
        config = _server_state.get("config", {})
        warehouse_id = config.get("sql_warehouse_id", "")

        source = body.get("source_catalog", config.get("source_catalog"))
        dest = body.get("destination_catalog", config.get("destination_catalog"))
        exclude = config.get("exclude_schemas", ["information_schema", "default"])

        result = validate_catalog(client, warehouse_id, source, dest, exclude)
        self._send_json(200, result)

    def _handle_metrics(self, params):
        queue = _server_state.get("queue")
        if not queue:
            self._send_json(200, {"metrics": {}})
            return

        jobs = queue.list_jobs()
        total = len(jobs)
        running = sum(1 for j in jobs if j.get("status") == "running")
        completed = sum(1 for j in jobs if j.get("status") == "completed")
        failed = sum(1 for j in jobs if j.get("status") == "failed")

        self._send_json(200, {
            "metrics": {
                "total_jobs": total,
                "running": running,
                "completed": completed,
                "failed": failed,
                "queued": total - running - completed - failed,
            }
        })


def start_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    config: dict | None = None,
    client=None,
    api_key: str | None = None,
):
    """Start the Clone-Xs API server."""
    global _server_state

    _server_state["config"] = config or {}
    _server_state["client"] = client
    _server_state["api_key"] = api_key

    # Initialize clone queue
    try:
        from src.clone_queue import CloneQueue
        queue = CloneQueue()
        _server_state["queue"] = queue

        # Start queue processor in background
        queue_thread = threading.Thread(target=queue.process_queue, daemon=True)
        queue_thread.start()
    except Exception as e:
        logger.warning(f"Could not initialize clone queue: {e}")

    server = HTTPServer((host, port), CloneAPIHandler)
    logger.info(f"Clone-Xs API server starting on {host}:{port}")
    logger.info(f"  Health check: http://{host}:{port}/api/health")
    logger.info(f"  API key: {'configured' if api_key else 'not required'}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("API server shutting down")
    finally:
        server.server_close()
