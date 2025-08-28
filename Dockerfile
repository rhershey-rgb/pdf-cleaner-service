# -------- Base image --------
FROM python:3.11-slim

# Avoid interactive tz prompts during apt
ARG DEBIAN_FRONTEND=noninteractive

# -------- System deps for pdfplumber + pandas --------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpoppler-cpp-dev \
    pkg-config \
    poppler-utils \
 && rm -rf /var/lib/apt/lists/*

# -------- Python deps --------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------- App code --------
COPY . .

# Good Python defaults
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Expose for local runs (Render will set $PORT)
EXPOSE 8080

# IMPORTANT: bind to $PORT in Render, default to 8080 locally
CMD ["sh","-c","uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"]
