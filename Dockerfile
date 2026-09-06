FROM node:22-alpine AS builder

WORKDIR /app

# Copy package manifests first to leverage Docker layer caching
COPY package*.json ./
COPY packages/core/package.json packages/core/
COPY apps/api/package.json apps/api/
COPY apps/worker/package.json apps/worker/

RUN npm ci

# Copy source and build
COPY . .
RUN npm run build -w packages/core && \
    npm run build -w apps/api && \
    npm run build -w apps/worker

# Runner stage
FROM node:22-alpine AS runner
WORKDIR /app

COPY --from=builder /app/package*.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/core/dist ./packages/core/dist
COPY --from=builder /app/packages/core/package.json ./packages/core/
COPY --from=builder /app/apps/api/dist ./apps/api/dist
COPY --from=builder /app/apps/api/package.json ./apps/api/
COPY --from=builder /app/apps/worker/dist ./apps/worker/dist
COPY --from=builder /app/apps/worker/package.json ./apps/worker/

# The CMD will be provided by docker-compose