FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:c79e0c910458f04e0e21902617e938d4b141e46c346f1a929834e0b1eac6df90 AS uv

WORKDIR /app

COPY uv.lock pyproject.toml ./

RUN uv export --no-group dev --frozen --no-build --no-emit-project > /app/requirements.txt

FROM python:3.12-slim@sha256:4718a47de1298c890b835bd5f4978acee04ba90f17c3395ee3d5e06920b63a26

WORKDIR /app
ENV PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

ENTRYPOINT ["python", "main.py"]

RUN apt-get update && apt-get install -y mediainfo ffmpeg &&\
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/*

COPY --from=uv /app/requirements.txt .

RUN pip install --only-binary=:all: --no-cache -r requirements.txt

COPY . .
