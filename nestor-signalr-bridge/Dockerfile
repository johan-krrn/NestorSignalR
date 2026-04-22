ARG BUILD_FROM
FROM ${BUILD_FROM}

# Metadata
LABEL maintainer="Nestor Smart Home"
LABEL description="Bidirectional bridge: Home Assistant <-> Azure SignalR"

# Install Node.js (HA base images are Alpine without Node)
RUN apk add --no-cache nodejs npm

# Create app directory
WORKDIR /app

# Copy dependency manifest first (Docker cache layer)
COPY package.json package-lock.json* ./

# Install production dependencies only
RUN npm ci --only=production --ignore-scripts && \
  npm cache clean --force

# Copy application code
COPY index.js ./

# Create s6-overlay service script inline (avoids Windows CRLF entirely)
RUN mkdir -p /etc/services.d/nestor-signalr-bridge && \
  printf '#!/bin/sh\nexec node /app/index.js\n' \
  > /etc/services.d/nestor-signalr-bridge/run && \
  chmod +x /etc/services.d/nestor-signalr-bridge/run
