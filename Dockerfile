FROM python:3.12-slim-bookworm AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY backend/requirements.lock ./
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/venv && /opt/venv/bin/pip install -r requirements.lock
COPY contracts /contracts
RUN /opt/venv/bin/pip install --no-deps /contracts
COPY backend/pyproject.toml ./
COPY backend/app ./app
RUN /opt/venv/bin/pip install --no-deps --no-cache-dir .

FROM python:3.12-slim-bookworm
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home \
      --shell /usr/sbin/nologin app \
    && mkdir -p /data && chown 10001:10001 /data
COPY --from=build /opt/venv /opt/venv
WORKDIR /service
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log", "--no-proxy-headers", "--timeout-graceful-shutdown", "25"]
