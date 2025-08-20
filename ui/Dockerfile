# UI Dockerfile - build React/Vite and serve with Nginx
FROM --platform=amd64 node:20-alpine AS build
WORKDIR /app

# Install deps
COPY ui/package.json ui/package-lock.json* ./
RUN npm ci || npm install

# Copy source
COPY ui/ ./

# Pass API base URL at build-time (default to /api so it works behind proxy)
ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

# Build
RUN npm run build

# 2) Runtime stage (serve static)
FROM --platform=amd64 nginx:1.27-alpine AS runtime

# Copy built assets
COPY --from=build /app/dist /usr/share/nginx/html

# Provide a basic Nginx config for static hosting (UI service only)
# Note: Reverse proxy is handled by separate proxy container
RUN rm -f /etc/nginx/conf.d/default.conf
COPY ui/nginx.ui.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
