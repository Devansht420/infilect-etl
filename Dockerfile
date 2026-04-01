FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev

# Copy app source
COPY ./app ./app

EXPOSE 8000

# Hot reload via uvicorn, run through uv
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]