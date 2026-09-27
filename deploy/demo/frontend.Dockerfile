FROM node:20-bookworm-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --include=dev
# The adjacent ignore file limits this copy to configuration and supporting files.
# Application source and artwork are mounted read-only by Compose.
COPY . .
