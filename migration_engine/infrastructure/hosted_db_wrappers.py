import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class HostedDBResolutionError(Exception):
    pass


@dataclass
class HostedDBRef:
    provider: str
    resource_id: str
    api_token: str = ""
    api_url: str = ""
    metadata: Optional[Dict[str, Any]] = None


class BaseHostedDBWrapper:
    provider_name = "base"

    def resolve(self, reference: HostedDBRef) -> Dict[str, Any]:
        raise NotImplementedError

    def _fetch_json(self, url: str, token: str = "") -> Dict[str, Any]:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(url, headers=headers)

        try:
            with urlopen(request, timeout=15) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            raise HostedDBResolutionError(
                f"{self.provider_name} API error {exc.code} while resolving hosted database"
            ) from exc
        except URLError as exc:
            raise HostedDBResolutionError(
                f"{self.provider_name} API is unreachable while resolving hosted database"
            ) from exc
        except json.JSONDecodeError as exc:
            raise HostedDBResolutionError(
                f"{self.provider_name} returned invalid JSON while resolving hosted database"
            ) from exc

    def _ensure_required(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        required = ("host", "port", "database", "username", "password")
        missing = [key for key in required if not normalized.get(key)]
        if missing:
            raise HostedDBResolutionError(
                f"{self.provider_name} response missing required fields: {', '.join(missing)}"
            )
        return normalized


class RenderHostedDBWrapper(BaseHostedDBWrapper):
    provider_name = "render"

    def resolve(self, reference: HostedDBRef) -> Dict[str, Any]:
        payload = (reference.metadata or {}).get("payload")
        if not payload:
            url = reference.api_url or os.getenv("RENDER_HOSTED_DB_API_URL", "")
            token = reference.api_token or os.getenv("RENDER_API_TOKEN", "")
            if not url:
                raise HostedDBResolutionError("render api_url is required when payload is not provided")
            payload = self._fetch_json(f"{url.rstrip('/')}/{reference.resource_id}", token)

        normalized = {
            "host": payload.get("host") or payload.get("hostname"),
            "port": payload.get("port"),
            "database": payload.get("database") or payload.get("dbName"),
            "username": payload.get("username") or payload.get("user"),
            "password": payload.get("password"),
            "ssl_mode": payload.get("ssl_mode") or "require",
            "uri": payload.get("uri") or payload.get("connectionString", ""),
        }
        return self._ensure_required(normalized)


class VercelHostedDBWrapper(BaseHostedDBWrapper):
    provider_name = "vercel"

    def resolve(self, reference: HostedDBRef) -> Dict[str, Any]:
        payload = (reference.metadata or {}).get("payload")
        if not payload:
            url = reference.api_url or os.getenv("VERCEL_HOSTED_DB_API_URL", "")
            token = reference.api_token or os.getenv("VERCEL_API_TOKEN", "")
            if not url:
                raise HostedDBResolutionError("vercel api_url is required when payload is not provided")
            payload = self._fetch_json(f"{url.rstrip('/')}/{reference.resource_id}", token)

        normalized = {
            "host": payload.get("host") or payload.get("hostname"),
            "port": payload.get("port"),
            "database": payload.get("database") or payload.get("dbName") or payload.get("name"),
            "username": payload.get("username") or payload.get("user"),
            "password": payload.get("password"),
            "ssl_mode": payload.get("ssl_mode") or "require",
            "uri": payload.get("uri") or payload.get("connectionString", ""),
        }
        return self._ensure_required(normalized)


class RailwayHostedDBWrapper(BaseHostedDBWrapper):
    provider_name = "railway"

    def resolve(self, reference: HostedDBRef) -> Dict[str, Any]:
        payload = (reference.metadata or {}).get("payload")
        if not payload:
            url = reference.api_url or os.getenv("RAILWAY_HOSTED_DB_API_URL", "")
            token = reference.api_token or os.getenv("RAILWAY_API_TOKEN", "")
            if not url:
                raise HostedDBResolutionError("railway api_url is required when payload is not provided")
            payload = self._fetch_json(f"{url.rstrip('/')}/{reference.resource_id}", token)

        normalized = {
            "host": payload.get("host") or payload.get("hostname") or payload.get("PGHOST"),
            "port": payload.get("port") or payload.get("PGPORT"),
            "database": payload.get("database") or payload.get("PGDATABASE"),
            "username": payload.get("username") or payload.get("user") or payload.get("PGUSER"),
            "password": payload.get("password") or payload.get("PGPASSWORD"),
            "ssl_mode": payload.get("ssl_mode") or "prefer",
            "uri": payload.get("uri") or payload.get("DATABASE_URL", ""),
        }
        return self._ensure_required(normalized)


class NetlifyHostedDBWrapper(BaseHostedDBWrapper):
    provider_name = "netlify"

    def resolve(self, reference: HostedDBRef) -> Dict[str, Any]:
        payload = (reference.metadata or {}).get("payload")
        if not payload:
            url = reference.api_url or os.getenv("NETLIFY_HOSTED_DB_API_URL", "")
            token = reference.api_token or os.getenv("NETLIFY_API_TOKEN", "")
            if not url:
                raise HostedDBResolutionError("netlify api_url is required when payload is not provided")
            payload = self._fetch_json(f"{url.rstrip('/')}/{reference.resource_id}", token)

        normalized = {
            "host": payload.get("host") or payload.get("hostname"),
            "port": payload.get("port"),
            "database": payload.get("database") or payload.get("dbName"),
            "username": payload.get("username") or payload.get("user"),
            "password": payload.get("password"),
            "ssl_mode": payload.get("ssl_mode") or "require",
            "uri": payload.get("uri") or payload.get("connectionString", ""),
        }
        return self._ensure_required(normalized)


def get_hosted_db_wrapper(provider: str) -> BaseHostedDBWrapper:
    provider_map = {
        "render": RenderHostedDBWrapper(),
        "vercel": VercelHostedDBWrapper(),
        "railway": RailwayHostedDBWrapper(),
        "netlify": NetlifyHostedDBWrapper(),
    }
    try:
        return provider_map[provider.lower()]
    except KeyError as exc:
        raise HostedDBResolutionError(f"unsupported hosted database provider: {provider}") from exc
