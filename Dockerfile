FROM ghcr.io/astral-sh/uv:python3.10-bookworm@sha256:da31df5ec7fba41b53ac7e72307f465b0cfddb76c16212366ef3d0df5328944e AS uv

WORKDIR /app

COPY uv.lock pyproject.toml ./

RUN uv export --no-group dev --frozen --no-build --no-emit-project > /app/requirements.txt

FROM python:3.10-slim@sha256:77f72bc159950ab208b8bbb9a7b22868984e004ce62966d1ca9268c346c73c64

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
