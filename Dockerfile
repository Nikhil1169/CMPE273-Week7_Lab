FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code.
COPY service_registry.py /app/service_registry.py
COPY client_service.py /app/client_service.py

# Security: run as a non-root user.
RUN useradd -m appuser
USER appuser

# Default: run the registry. Override APP at runtime to run the client service.
# Example:
#   docker run -e APP=client_service:app -e SERVICE_PORT=8001 -p 8001:8001 ...
ENV APP=service_registry:app \
    HOST=0.0.0.0 \
    PORT=8000

EXPOSE 8000

CMD ["sh", "-c", "uvicorn ${APP} --host ${HOST} --port ${PORT}"]

