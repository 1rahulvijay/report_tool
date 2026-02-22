# 1. Builder Stage (Node & Reflex Init)
FROM python:3.11-slim as builder

# Install System Dependencies and Node.js
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unzip \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG http_proxy
ARG https_proxy
ARG npm_config_registry

# Setup Frontend for building
COPY frontend/requirements.txt ./frontend/
RUN pip install --no-cache-dir -r frontend/requirements.txt

COPY frontend/ ./frontend/
WORKDIR /app/frontend

RUN reflex init && reflex export --frontend-only

# 2. Final Runtime Stage
FROM python:3.11-slim

# Install Nginx
RUN apt-get update && apt-get install -y \
    nginx \
    && rm -rf /var/lib/apt/lists/* \
    && chown -R 1001:0 /var/lib/nginx /var/log/nginx /run

WORKDIR /app

# Setup Backend & Frontend Python dependencies
COPY backend/requirements.txt ./backend/
COPY frontend/requirements.txt ./frontend/
RUN pip install --no-cache-dir -r backend/requirements.txt -r frontend/requirements.txt

# Copy Source Code & Configs
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY entrypoint.sh nginx.conf ./

# Copy built frontend assets from builder
COPY --from=builder /app/frontend/.web /app/frontend/.web

# Final preparation for OpenShift (Set Permissions & User)
RUN chmod +x entrypoint.sh && \
    chown -R 1001:0 /app && \
    chmod -R g=u /app

USER 1001

# Expose ports: 8081 (Nginx Reverse Proxy)
EXPOSE 8081

ENTRYPOINT ["./entrypoint.sh"]
