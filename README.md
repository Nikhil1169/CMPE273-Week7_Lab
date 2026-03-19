# Build a Microservice with Discovery (FastAPI)

This repository contains an educational **service discovery** microservice system implemented with **FastAPI**.

- `service_registry.py` acts as a central registry where service instances **register**, **deregister**, send **heartbeats**, and can be **discovered**.
- `client_service.py` is a microservice that plays two roles at once:
  - It **registers** itself (and heartbeats) with the registry on startup.
  - It can **discover** other registered instances and **route** calls to a randomly chosen instance via `/consume`.

The project also includes a clean **Dockerfile**, **Kubernetes manifests**, and an optional **Istio** example for “service mesh” style routing.

## Assignment Deliverables Checklist

- Run **2 service instances**
- Register with the central registry (via `/v1/register` on startup)
- Client discovers a service from the registry (via `/v1/discover/{service_name}`)
- Client calls a **random instance** of the discovered service (via `/consume`)
- Architecture diagram: see [ARCHITECTURE.md](./ARCHITECTURE.md)
- Optional Bonus: Service Mesh Discovery (Istio manifests in `k8s/`)

## Local Quick Start (How to Run)

### 1) Set up the virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Start the registry

Terminal A:

```bash
uvicorn service_registry:app --host 0.0.0.0 --port 8000
```

### 3) Start service instance 1

Terminal B:

```bash
uvicorn client_service:app --host 0.0.0.0 --port 8001
```

### 4) Start service instance 2

Terminal C:

```bash
uvicorn client_service:app --host 0.0.0.0 --port 8002
```

### 5) Test random load balancing (client discovery + random routing)

Open another terminal and run:

```bash
curl http://localhost:8001/consume
```

Repeat the command a few times (and optionally run it against port `8002`) to observe that `/consume` randomly selects among the discovered instances and forwards the request to their `/hello` endpoint.

## Docker & Kubernetes Deployment

### Docker

Build the image:

```bash
docker build -t discovery-demo:latest .
```

Run the registry container:

```bash
docker run --rm -p 8000:8000 -e APP=service_registry:app -e PORT=8000 discovery-demo:latest
```

Run service instances (two examples):

```bash
docker run --rm -p 8001:8001 \
  -e APP=client_service:app -e PORT=8001 -e SERVICE_PORT=8001 \
  -e REGISTRY_URL="http://host.docker.internal:8000" \
  -e PUBLIC_BASE_URL="http://host.docker.internal:8001" \
  discovery-demo:latest
```

```bash
docker run --rm -p 8002:8002 \
  -e APP=client_service:app -e PORT=8002 -e SERVICE_PORT=8002 \
  -e REGISTRY_URL="http://host.docker.internal:8000" \
  -e PUBLIC_BASE_URL="http://host.docker.internal:8002" \
  discovery-demo:latest
```

### Kubernetes

Apply the manifests:

```bash
kubectl apply -f k8s/deployment.yaml
```

This deploys:

- 1 replica of the registry
- 2 replicas of the client service (which self-registers and heartbeats)

### Optional Bonus: Istio Service Mesh Discovery

To demonstrate an Istio routing manifest, apply:

```bash
kubectl apply -f k8s/istio-service-mesh.yaml
```

The Istio example includes a `DestinationRule` and `VirtualService` for traffic steering.

