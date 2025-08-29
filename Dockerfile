FROM python:3.11-slim
ARG DEBIAN_FRONTEND=noninteractive

# deps for pdfplumber
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpoppler-cpp-dev pkg-config poppler-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
EXPOSE 8080

# Keep a single worker; the in-app lock enforces one-at-a-time processing.
# Tip: set Render Health Check Path to /healthz
CMD ["sh","-c","uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1 --limit-concurrency 2 --timeout-keep-alive 5"]
