FROM python:3.11-slim

# Metadata
LABEL maintainer="EnPro Filtration Mastermind"
LABEL description="AI-powered filtration product portal"

# Create non-root user
RUN groupadd -r enpro && useradd -r -g enpro -d /app -s /sbin/nologin enpro

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p /app/data/sessions && chown -R enpro:enpro /app

# Switch to non-root user
USER enpro

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import httpx; r = httpx.get('http://localhost:8000/health'); assert r.status_code == 200"

# Run with single worker (stateful app — in-memory DataFrames)
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
