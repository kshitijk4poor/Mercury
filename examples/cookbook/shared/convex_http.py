"""Minimal Convex HTTP client used by Mercury cookbook examples."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, request


class ConvexHTTPError(RuntimeError):
    """Raised when Convex returns an HTTP or function-level error."""


@dataclass(frozen=True)
class ConvexSettings:
    url: str
    access_token: str | None = None

    @classmethod
    def from_env(cls) -> "ConvexSettings":
        url = os.environ.get("CONVEX_URL")
        if not url:
            raise ValueError("CONVEX_URL is required")
        return cls(
            url=url.rstrip("/"),
            access_token=os.environ.get("CONVEX_ACCESS_TOKEN"),
        )


class ConvexHTTPClient:
    """Tiny wrapper around Convex `/api/query|mutation|action` endpoints."""

    def __init__(self, settings: ConvexSettings):
        self._settings = settings

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._settings.access_token:
            headers["Authorization"] = f"Bearer {self._settings.access_token}"
        return headers

    def _call(self, endpoint: str, *, path: str, args: dict[str, Any]) -> Any:
        url = f"{self._settings.url}/api/{endpoint}"
        payload = {
            "path": path,
            "args": args,
            "format": "json",
        }
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url, data=body, headers=self._headers(), method="POST")

        try:
            with request.urlopen(req, timeout=30) as res:
                data = json.loads(res.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ConvexHTTPError(
                f"Convex {endpoint} failed: HTTP {exc.code} {detail}"
            ) from exc
        except error.URLError as exc:
            raise ConvexHTTPError(f"Convex {endpoint} failed: {exc.reason}") from exc

        if isinstance(data, dict) and data.get("status") == "error":
            raise ConvexHTTPError(
                f"Convex function error ({endpoint} {path}): {data.get('errorMessage', data)}"
            )

        if isinstance(data, dict) and "value" in data:
            return data["value"]
        return data

    def query(self, path: str, args: dict[str, Any] | None = None) -> Any:
        return self._call("query", path=path, args=args or {})

    def mutation(self, path: str, args: dict[str, Any] | None = None) -> Any:
        return self._call("mutation", path=path, args=args or {})

    def action(self, path: str, args: dict[str, Any] | None = None) -> Any:
        return self._call("action", path=path, args=args or {})
