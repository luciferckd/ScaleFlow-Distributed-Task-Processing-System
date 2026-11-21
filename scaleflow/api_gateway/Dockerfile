FROM node:22-alpine AS runner

# Production environment
ENV NODE_ENV=production

WORKDIR /app

# Install production dependencies only with layer caching
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Copy application source code
COPY --chown=node:node src ./src

# Run as unprivileged non-root user
USER node

EXPOSE 5000

# Execute server directly so OS signals (SIGTERM/SIGINT) are handled cleanly
CMD ["node", "src/server.js"]


