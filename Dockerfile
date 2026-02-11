FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /build

COPY pyproject.toml uv.lock ./
COPY kubetimer/ ./kubetimer/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev


FROM python:3.14-slim-trixie

RUN groupadd -r kopf && useradd -r -g kopf kopf
USER kopf

WORKDIR /app

COPY --from=builder /build/.venv /app/.venv
COPY kubetimer/ ./kubetimer/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app"

EXPOSE 8080

CMD ["python", "kubetimer/main.py"] 