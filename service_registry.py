"""
Central Service Registry (FastAPI).

This service implements a tiny, in-memory service registry suitable for
learning service discovery concepts:

- Service instances register themselves on startup.
- Instances periodically send heartbeats.
- Clients discover currently-live instances for a given service name.
- The registry automatically cleans up stale instances via an asyncio
  background task (TTL-based).

NOTE: This is intentionally kept simple (no persistence, no auth) to match
an educational assignment.
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from fastapi import FastAPI, HTTPException, Path, status
from pydantic import BaseModel, Field, HttpUrl


def _utc_epoch_seconds() -> float:
    # Using epoch seconds avoids timezone pitfalls and is easy to compare.
    return time.time()


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


REGISTRY_CLEANUP_INTERVAL_SECONDS = _env_int("REGISTRY_CLEANUP_INTERVAL_SECONDS", 5, min_value=1, max_value=300)
REGISTRY_DEFAULT_TTL_SECONDS = _env_int("REGISTRY_DEFAULT_TTL_SECONDS", 15, min_value=3, max_value=300)


class RegisterRequest(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    instance_id: str = Field(..., min_length=1, max_length=256)
    base_url: HttpUrl
    ttl_seconds: Optional[int] = Field(
        default=None,
        description="How long (in seconds) this instance is considered alive without a heartbeat.",
        ge=3,
        le=300,
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


class HeartbeatRequest(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    instance_id: str = Field(..., min_length=1, max_length=256)


class DeregisterRequest(BaseModel):
    service_name: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
    instance_id: str = Field(..., min_length=1, max_length=256)


class InstanceView(BaseModel):
    service_name: str
    instance_id: str
    base_url: HttpUrl
    metadata: Dict[str, Any]
    registered_at_epoch_s: float
    last_heartbeat_at_epoch_s: float
    ttl_seconds: int


class DiscoverResponse(BaseModel):
    service_name: str
    instances: List[InstanceView]
    count: int


@dataclass
class _Instance:
    service_name: str
    instance_id: str
    base_url: str
    metadata: Dict[str, Any]
    registered_at_epoch_s: float
    last_heartbeat_at_epoch_s: float
    ttl_seconds: int

    def to_view(self) -> InstanceView:
        return InstanceView(
            service_name=self.service_name,
            instance_id=self.instance_id,
            base_url=self.base_url,  # pydantic will validate URL formatting
            metadata=self.metadata,
            registered_at_epoch_s=self.registered_at_epoch_s,
            last_heartbeat_at_epoch_s=self.last_heartbeat_at_epoch_s,
            ttl_seconds=self.ttl_seconds,
        )


class ServiceRegistryStore:
    """
    An in-memory registry of services -> instances.

    Structure:
      services[service_name][instance_id] = _Instance
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._services: Dict[str, Dict[str, _Instance]] = {}

    async def register(self, req: RegisterRequest) -> _Instance:
        now = _utc_epoch_seconds()
        ttl = req.ttl_seconds or REGISTRY_DEFAULT_TTL_SECONDS

        instance = _Instance(
            service_name=req.service_name,
            instance_id=req.instance_id,
            base_url=str(req.base_url).rstrip("/"),
            metadata=req.metadata,
            registered_at_epoch_s=now,
            last_heartbeat_at_epoch_s=now,
            ttl_seconds=ttl,
        )

        async with self._lock:
            svc = self._services.setdefault(req.service_name, {})
            # Upsert: if an instance restarts with the same instance_id, we refresh it.
            svc[req.instance_id] = instance
        return instance

    async def heartbeat(self, req: HeartbeatRequest) -> _Instance:
        now = _utc_epoch_seconds()
        async with self._lock:
            svc = self._services.get(req.service_name)
            if not svc or req.instance_id not in svc:
                raise KeyError("instance_not_registered")
            svc[req.instance_id].last_heartbeat_at_epoch_s = now
            return svc[req.instance_id]

    async def deregister(self, req: DeregisterRequest) -> bool:
        async with self._lock:
            svc = self._services.get(req.service_name)
            if not svc:
                return False
            existed = svc.pop(req.instance_id, None) is not None
            if not svc:
                # remove empty service bucket
                self._services.pop(req.service_name, None)
            return existed

    async def discover(self, service_name: str) -> List[_Instance]:
        now = _utc_epoch_seconds()
        async with self._lock:
            svc = self._services.get(service_name, {})
            # Return only currently-live instances; cleanup loop also removes stale,
            # but this ensures discovery is safe even between cleanup ticks.
            live: List[_Instance] = []
            for inst in svc.values():
                if (now - inst.last_heartbeat_at_epoch_s) <= inst.ttl_seconds:
                    live.append(inst)
            return live

    async def snapshot(self) -> Mapping[str, Mapping[str, _Instance]]:
        async with self._lock:
            # Shallow copy is enough for a debug view (instances are dataclasses).
            return {svc: dict(instances) for svc, instances in self._services.items()}

    async def cleanup_stale(self) -> int:
        """
        Remove stale instances (TTL expired).
        Returns the number of removed instances.
        """
        now = _utc_epoch_seconds()
        removed = 0
        async with self._lock:
            for service_name in list(self._services.keys()):
                instances = self._services[service_name]
                for instance_id in list(instances.keys()):
                    inst = instances[instance_id]
                    if (now - inst.last_heartbeat_at_epoch_s) > inst.ttl_seconds:
                        instances.pop(instance_id, None)
                        removed += 1
                if not instances:
                    self._services.pop(service_name, None)
        return removed


store = ServiceRegistryStore()


async def _cleanup_loop(stop_event: asyncio.Event) -> None:
    """
    Background task: periodically remove stale instances.

    Uses an Event to support quick, clean shutdown.
    """
    try:
        while not stop_event.is_set():
            await store.cleanup_stale()
            # Wait with wake-up on stop
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=REGISTRY_CLEANUP_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        # FastAPI lifespan cancellation: exit quickly.
        return


@asynccontextmanager
async def lifespan(_: FastAPI):
    stop_event = asyncio.Event()
    task = asyncio.create_task(_cleanup_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        task.cancel()
        with suppress(Exception):
            await task


app = FastAPI(
    title="Service Registry",
    version="1.0.0",
    description="Educational service discovery registry (FastAPI).",
    lifespan=lifespan,
)


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/register", response_model=InstanceView, status_code=status.HTTP_201_CREATED)
async def register(req: RegisterRequest) -> InstanceView:
    """
    Register (or refresh) a service instance.

    Services call this on startup. The registry stores the instance and marks it alive.
    """
    instance = await store.register(req)
    return instance.to_view()


@app.post("/v1/heartbeat", response_model=InstanceView)
async def heartbeat(req: HeartbeatRequest) -> InstanceView:
    """
    Heartbeat updates liveness of a registered instance.
    """
    try:
        instance = await store.heartbeat(req)
    except KeyError:
        # Client can choose to re-register if it gets 404 here.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instance not registered. Register first, then send heartbeats.",
        )
    return instance.to_view()


@app.post("/v1/deregister", status_code=status.HTTP_200_OK)
async def deregister(req: DeregisterRequest) -> Dict[str, Any]:
    """
    Deregister is best-effort and should be called during graceful shutdown.
    """
    existed = await store.deregister(req)
    return {"status": "deregistered" if existed else "not_found", "service_name": req.service_name, "instance_id": req.instance_id}


@app.get("/v1/discover/{service_name}", response_model=DiscoverResponse)
async def discover(
    service_name: str = Path(..., min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")
) -> DiscoverResponse:
    """
    Discover live instances for a given service name.
    """
    instances = await store.discover(service_name)
    views = [i.to_view() for i in instances]
    return DiscoverResponse(service_name=service_name, instances=views, count=len(views))


@app.get("/v1/services")
async def list_services() -> Dict[str, Any]:
    """
    Debug endpoint: list all services and instances currently known to the registry.
    """
    snap = await store.snapshot()
    out: Dict[str, Any] = {}
    for service_name, instances in snap.items():
        out[service_name] = {
            "count": len(instances),
            "instances": [inst.to_view().model_dump() for inst in instances.values()],
        }
    return {"services": out}

