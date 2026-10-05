FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY Silver.py .

# Secrets are NOT baked into the image (.env and the service account
# JSON are excluded by .dockerignore). Provide them at runtime:
#
#   docker run --rm --env-file .env \
#     -v "$(pwd)/google-service-account.json:/app/google-service-account.json:ro" \
#     silver3599
#
# --env-file loads every variable from .env into the container, which
# Silver.py reads via os.getenv (load_dotenv is a no-op without a file).

RUN useradd --create-home appuser
USER appuser

CMD ["python", "Silver.py"]
