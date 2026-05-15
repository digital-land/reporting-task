# syntax=docker/dockerfile:1

FROM python:3.8-slim-bookworm
WORKDIR /src

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    make sqlite3 gdal-bin libsqlite3-mod-spatialite build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    apt-get remove -y build-essential && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["./run.sh"]

