# Backend Dockerfile
FROM --platform=amd64 python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps for weasyprint, matplotlib, and building wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libcairo2 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    libfreetype6 \
    libpng16-16 \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first to leverage caching
COPY backend/requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy backend source
COPY backend/app ./app
# Also copy project-level modules used by backend (if imported)
COPY cell_change_evolution ./cell_change_evolution
COPY cell_change_processor.py ./cell_change_processor.py

EXPOSE 8000

# Default environment (override in docker-compose)
ENV API_PORT=8000

# Start FastAPI with uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
