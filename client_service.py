"""
Client microservice (FastAPI) demonstrating service discovery.

This single microservice can play two roles:
1) A *callee* (it exposes `/hello` so other services can call it)
2) A *caller* (it exposes `/consume` that discovers a target service from
   the registry, picks a random instance, and calls its `/hello` endpoint)

Each running instance:
- Registers itself with the central registry at startup
- Sends periodic heartbeats in a background asyncio task
- Deregisters itself on graceful shutdown (best effort)

This fulfills the assignment requirements:
- Run 2 instances (e.g., on ports 8001 and 8002)
- Central registry provides discovery
- `/consume` randomly selects an instance and routes to it
"""

from __future__ import annotations

import asyncio
import os
import random
import socket
import sys
import time
import uuid
from contextlib import asynccontextmanager, suppress
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, HttpUrl


def _utc_epoch_seconds() -> float:
    return time.time()


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or v == "" else v


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 3600) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < min_value or value > max_value:
        raise RuntimeError(f"{name} must be in [{min_value}, {max_value}], got {value}")
    return value


def _port_from_argv(default: int) -> int:
    """
    Detect a uvicorn port passed via CLI arguments.

    Supports:
    - uvicorn client_service:app --port 8002
    - uvicorn client_service:app --port=8002

    If not present or invalid, returns `default`.
    """
    argv = sys.argv[1:]
    for i, arg in enumerate(argv):
        if arg == "--port":
            if i + 1 >= len(argv):
                return default
            candidate = argv[i + 1]
        elif arg.startswith("--port="):
            candidate = arg.split("=", 1)[1]
        else:
            continue

        try:
            port = int(candidate)
        except ValueError:
            return default
        if 1 <= port <= 65535:
            return port
        return default
    return default


SERVICE_NAME = _env_str("SERVICE_NAME", "demo-service")
INSTANCE_ID = _env_str("INSTANCE_ID", str(uuid.uuid4()))
REGISTRY_URL = _env_str("REGISTRY_URL", "http://localhost:8000").rstrip("/")
TARGET_SERVICE_NAME = _env_str("TARGET_SERVICE_NAME", SERVICE_NAME)

HEARTBEAT_INTERVAL_SECONDS = _env_int("HEARTBEAT_INTERVAL_SECONDS", 5, min_value=1, max_value=120)
INSTANCE_TTL_SECONDS = _env_int("INSTANCE_TTL_SECONDS", 15, min_value=3, max_value=300)

# Prefer explicit public URL (best for containers/Kubernetes); fall back to host+port.
PUBLIC_BASE_URL = _env_str("PUBLIC_BASE_URL", "").rstrip("/")
SERVICE_HOST = _env_str("SERVICE_HOST", "localhost")
SERVICE_PORT = _env_int("SERVICE_PORT", _port_from_argv(8001), min_value=1, max_value=65535)


def _compute_base_url() -> str:
    """
    This is the address other services will use to call this instance.
    - Locally: http://localhost:8001, http://localhost:8002, ...
    - In Kubernetes: best provided via PUBLIC_BASE_URL (or service DNS + pod port)
    """
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return f"http://{SERVICE_HOST}:{SERVICE_PORT}"


BASE_URL = _compute_base_url()


class _RegisterPayload(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=128)
    instance_id: str = Field(..., min_length=1, max_length=256)
    base_url: HttpUrl
    ttl_seconds: int = Field(..., ge=3, le=300)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class _HeartbeatPayload(BaseModel):
    service_name: str
    instance_id: str


class _DeregisterPayload(BaseModel):
    service_name: str
    instance_id: str


class _DiscoverResponse(BaseModel):
    service_name: str
    instances: list[dict[str, Any]]
    count: int


def _default_metadata() -> Dict[str, Any]:
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
    }


async def _post_json_with_retries(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    *,
    timeout_s: float = 3.0,
    attempts: int = 3,
    base_backoff_s: float = 0.25,
) -> httpx.Response:
    """
    Small retry helper for registry calls.

    We only retry on transient network errors and 5xx responses.
    """
    last_exc: Optional[BaseException] = None
    for i in range(attempts):
        try:
            resp = await client.post(url, json=payload, timeout=timeout_s)
            if 500 <= resp.status_code <= 599:
                raise httpx.HTTPStatusError("registry 5xx", request=resp.request, response=resp)
            return resp
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            if i == attempts - 1:
                break
            # jittered exponential backoff
            sleep_for = base_backoff_s * (2**i) * (0.8 + random.random() * 0.4)
            await asyncio.sleep(sleep_for)
    raise RuntimeError(f"Registry request failed after {attempts} attempts: {last_exc!r}") from last_exc


async def _register_self(client: httpx.AsyncClient) -> None:
    payload = _RegisterPayload(
        service_name=SERVICE_NAME,
        instance_id=INSTANCE_ID,
        base_url=BASE_URL,
        ttl_seconds=INSTANCE_TTL_SECONDS,
        metadata=_default_metadata(),
    ).model_dump(mode="json")
    await _post_json_with_retries(client, f"{REGISTRY_URL}/v1/register", payload)


async def _deregister_self(client: httpx.AsyncClient) -> None:
    payload = _DeregisterPayload(service_name=SERVICE_NAME, instance_id=INSTANCE_ID).model_dump(mode="json")
    # best-effort; do not raise on failure during shutdown
    with suppress(Exception):
        await client.post(f"{REGISTRY_URL}/v1/deregister", json=payload, timeout=2.0)


async def _heartbeat_loop(stop_event: asyncio.Event, client: httpx.AsyncClient) -> None:
    """
    Background heartbeat loop.

    If the registry forgets the instance (404), we re-register and continue.
    """
    payload = _HeartbeatPayload(service_name=SERVICE_NAME, instance_id=INSTANCE_ID).model_dump(mode="json")
    while not stop_event.is_set():
        try:
            resp = await client.post(f"{REGISTRY_URL}/v1/heartbeat", json=payload, timeout=2.0)
            if resp.status_code == 404:
                # Registry restarted or evicted us; re-register.
                await _register_self(client)
            elif 500 <= resp.status_code <= 599:
                # transient server error; ignore and try again next tick
                pass
            elif resp.is_error:
                # For 4xx other than 404, surface the error by re-registering once.
                await _register_self(client)
        except httpx.RequestError:
            # Registry temporarily unreachable; keep running and retry next tick.
            pass

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan hook.

    We create a single AsyncClient for the app, register on startup,
    then keep heartbeating until shutdown, where we deregister best-effort.
    """
    async with httpx.AsyncClient(headers={"User-Agent": f"{SERVICE_NAME}/{INSTANCE_ID}"}) as client:
        stop_event = asyncio.Event()
        heartbeat_task: Optional[asyncio.Task[None]] = None
        try:
            # Register on startup (fail fast if registry is unavailable).
            await _register_self(client)
            heartbeat_task = asyncio.create_task(_heartbeat_loop(stop_event, client))
            # Make the client available to request handlers.
            app.state.http = client
            yield
        finally:
            stop_event.set()
            if heartbeat_task:
                heartbeat_task.cancel()
                with suppress(Exception):
                    await heartbeat_task
            await _deregister_self(client)
            with suppress(Exception):
                delattr(app.state, "http")


app = FastAPI(
    title="Client Service",
    version="1.0.0",
    description="Educational microservice with registry-based discovery (FastAPI).",
    lifespan=lifespan,
)


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"status": "ok", "service_name": SERVICE_NAME, "instance_id": INSTANCE_ID, "base_url": BASE_URL}


@app.get("/hello")
async def hello(caller: str = Query(default="unknown")) -> Dict[str, Any]:
    """
    A simple endpoint that other services can call.
    """
    return {
        "message": "hello",
        "service_name": SERVICE_NAME,
        "instance_id": INSTANCE_ID,
        "base_url": BASE_URL,
        "caller": caller,
        "ts_epoch_s": _utc_epoch_seconds(),
    }


@app.get("/consume")
async def consume(
    target_service: str = Query(default=TARGET_SERVICE_NAME, description="Service name to discover and call."),
    path: str = Query(default="/hello", description="Path to call on the discovered instance."),
) -> Dict[str, Any]:
    """
    Discover `target_service` from the registry, randomly choose a live instance,
    and call it via HTTP.
    """
    http: httpx.AsyncClient = getattr(app.state, "http", None)
    if http is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready yet.")

    # 1) Discover instances
    try:
        resp = await http.get(f"{REGISTRY_URL}/v1/discover/{target_service}", timeout=3.0)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Registry unreachable at {REGISTRY_URL}: {exc.__class__.__name__}",
        )

    if resp.status_code == 404:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Service not found: {target_service}")
    if resp.is_error:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Registry error: {resp.text}")

    data = _DiscoverResponse.model_validate(resp.json())
    if not data.instances:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"No live instances for service {target_service!r}. Wait for registration/heartbeats.",
        )

    # 2) Pick a random instance and call it
    chosen = random.choice(data.instances)
    base_url = str(chosen.get("base_url", "")).rstrip("/")
    instance_id = str(chosen.get("instance_id", "unknown"))
    url = f"{base_url}{path if path.startswith('/') else '/' + path}"

    try:
        upstream = await http.get(url, params={"caller": f"{SERVICE_NAME}/{INSTANCE_ID}"}, timeout=3.0)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed calling chosen instance {instance_id} at {url}: {exc.__class__.__name__}",
        )

    return {
        "registry_url": REGISTRY_URL,
        "target_service": target_service,
        "discovered_count": data.count,
        "chosen_instance_id": instance_id,
        "chosen_base_url": base_url,
        "called_url": url,
        "upstream_status_code": upstream.status_code,
        "upstream_json": upstream.json()
        if upstream.headers.get("content-type", "").startswith("application/json")
        else upstream.text,
    }

