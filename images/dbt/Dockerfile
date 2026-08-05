FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "dbt-trino==1.10.3" "dbt-core==1.12.0"
RUN useradd -m -u 1001 dbt
USER dbt
WORKDIR /home/dbt
ENTRYPOINT ["dbt"]
