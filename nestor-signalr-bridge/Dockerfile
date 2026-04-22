FROM node:20-alpine

# Metadata
LABEL maintainer="Nestor Smart Home"
LABEL description="Bidirectional bridge: Home Assistant <-> Azure SignalR"

# Create app directory
WORKDIR /app

# Copy dependency manifest first (Docker cache layer)
COPY package.json package-lock.json* ./

# Install production dependencies only
RUN npm ci --only=production --ignore-scripts && \
  npm cache clean --force

# Copy application code
COPY index.js ./

CMD ["node", "/app/index.js"]
