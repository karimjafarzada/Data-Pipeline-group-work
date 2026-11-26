FROM python:3.12-slim-bookworm AS build
WORKDIR /app
COPY pyproject.toml ./
COPY src/ src/
RUN python -m venv /venv \
    && /venv/bin/python -m pip install --no-cache-dir -e .

FROM python:3.12-slim-bookworm
RUN groupadd --system --gid 10001 pipeline \
    && useradd --system --uid 10001 --gid 10001 --no-create-home pipeline
WORKDIR /app
COPY --from=build /venv /venv
# The editable install resolves the source at /app/src, beside the migrations.
COPY src/ src/
COPY migrations/ migrations/
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
HEALTHCHECK --interval=30s --timeout=8s --retries=3 CMD ["python", "-c", "from pipeline.db import get_connection; get_connection(attempts=1).close()"]
CMD ["python", "-m", "pipeline", "--help"]
