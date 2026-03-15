# Stage 1: Build frontend
FROM node:20-slim AS frontend
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json* ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# Stage 2: Python backend + built frontend
FROM python:3.13-slim
WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY src/ ./src/
COPY api/ ./api/
COPY config/ ./config/
COPY --from=frontend /app/ui/dist ./ui/dist

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
