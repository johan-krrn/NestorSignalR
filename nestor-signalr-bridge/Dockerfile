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
COPY index.js run.sh ./

# Make startup script executable
RUN chmod a+x /app/run.sh

# Run as non-root where possible (HA add-ons run as root by default)
CMD ["/app/run.sh"]
