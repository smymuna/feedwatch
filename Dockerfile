# 1) Build the dashboard
FROM node:22-alpine AS dashboard
WORKDIR /dash
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

# 2) The Python services (one image, the command picks the service)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .
COPY scenarios ./scenarios
COPY samples ./samples
COPY --from=dashboard /dash/dist ./dashboard/dist
RUN useradd --create-home app && mkdir -p state data && chown -R app /app
USER app
ENTRYPOINT ["feedwatch"]
CMD ["--help"]
