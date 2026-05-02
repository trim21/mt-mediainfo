FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS uv

FROM python:3.12-slim@sha256:46cb7cc2877e60fbd5e21a9ae6115c30ace7a077b9f8772da879e4590c18c2e3

COPY --from=uv /usr/local/bin/uv /bin/uv

WORKDIR /src
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/src \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system \
    PATH="/src/.venv/bin:$PATH" \
    DATA_DIR=/data

VOLUME ["/data"]

ENTRYPOINT ["python", "main.py"]

RUN apt-get update && apt-get install -y mediainfo ffmpeg && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev --frozen --no-install-project --no-build --no-cache

COPY . .

ARG VERSION=""
ENV APP_VERSION=$VERSION
