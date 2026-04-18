FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS uv

FROM python:3.12-slim@sha256:804ddf3251a60bbf9c92e73b7566c40428d54d0e79d3428194edf40da6521286

COPY --from=uv /usr/local/bin/uv /bin/uv

WORKDIR /src
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/src \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system \
    PATH="/src/.venv/bin:$PATH"

ENTRYPOINT ["python", "main.py"]

RUN apt-get update && apt-get install -y mediainfo ffmpeg && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev --frozen --no-install-project --no-build --no-cache

COPY . .
