"""WebSocket connection manager for broadcasting progress updates."""

import json
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections grouped by job_id."""

    def __init__(self):
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        if job_id not in self._connections:
            self._connections[job_id] = []
        self._connections[job_id].append(websocket)
        logger.info(f"WS connected: job={job_id}, total={len(self._connections[job_id])}")

    def disconnect(self, websocket: WebSocket, job_id: str):
        if job_id in self._connections:
            self._connections[job_id] = [ws for ws in self._connections[job_id] if ws != websocket]
            if not self._connections[job_id]:
                del self._connections[job_id]

    async def broadcast(self, job_id: str, data: dict):
        """Send data to all clients watching a job."""
        if job_id not in self._connections:
            return
        dead = []
        for ws in self._connections[job_id]:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, job_id)
