FROM node:22.19.0-bookworm-slim
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@10.21.0 --activate
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
COPY schemas ./schemas
COPY connect/clickhouse-sink.json ./connect/clickhouse-sink.json
RUN pnpm run build
CMD ["node", "dist/produce.js"]
