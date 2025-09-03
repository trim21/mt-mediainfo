FROM ghcr.io/astral-sh/uv:python3.10-bookworm@sha256:bd16e6592706f4e7c5e9a87cb00fc5938b087e96ecd645fd43fabf8cb15902ff AS uv

WORKDIR /app

COPY uv.lock pyproject.toml ./

RUN uv export --no-group dev --frozen --no-build --no-emit-project > /app/requirements.txt

FROM python:3.10-slim@sha256:420fbb0e468d3eaf0f7e93ea6f7a48792cbcadc39d43ac95b96bee2afe4367da

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
