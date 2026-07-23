FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim@sha256:7cf77f594be8042dab6daa9fe326f90962252268b4f120a7f5dccce4d947e6c1 AS uv

FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6

COPY --from=uv /usr/local/bin/uv /bin/uv

WORKDIR /src
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/src \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system \
    PATH="/src/.venv/bin:$PATH" \
    DATA_DIR=/data \
    ONNXRUNTIME_DISABLE_GPU_DETECTION=1

VOLUME ["/data"]

ENTRYPOINT ["python", "main.py"]

RUN apt-get update && apt-get install -y mediainfo ffmpeg postgresql-client libicu76 util-linux && \
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

ADD https://github.com/tetrahydroc/BDInfoCLI/releases/download/v1.0.5/BDInfo-linux-x64.tar.gz /tmp/bdinfo.tar.gz
RUN tar --strip-components=1 -xzf /tmp/bdinfo.tar.gz -C /usr/local/bin/ && \
    chmod +x /usr/local/bin/BDInfo && \
    rm /tmp/bdinfo.tar.gz

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev --frozen --no-install-project --no-build --no-cache

COPY . .

ARG VERSION=""
ENV APP_VERSION=$VERSION
