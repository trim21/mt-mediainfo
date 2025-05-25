FROM ghcr.io/astral-sh/uv:python3.10-bookworm@sha256:be9448497a45e8cc1929c71408e876d721263f44175e07d42c1c540cf7e87f51 AS uv

WORKDIR /app

COPY uv.lock pyproject.toml ./

RUN uv export --no-group dev --frozen --no-build --no-emit-project > /app/requirements.txt

FROM python:3.10-slim@sha256:49454d2bf78a48f217eb25ecbcb4b5face313fea6a6e82706465a6990303ada2

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
