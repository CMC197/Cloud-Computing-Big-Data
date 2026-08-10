# =====================================================================
# AP4 — User-facing UI (React / Vite)
# Multi-Stage: Node baut, Nginx serviert. Kleines Runtime-Image,
# kein Node im Cluster.
# =====================================================================

# ---------- Stage 1: Build ----------
FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# ---------- Stage 2: Serve ----------
FROM nginx:1.27-alpine

# SPA-Routing: alles auf index.html umleiten, sonst 404 beim Reload
COPY nginx.conf /etc/nginx/conf.d/default.conf

COPY --from=build /app/dist /usr/share/nginx/html

# ⚠️ Die API-URL darf NICHT ins Image gebacken werden — sie steht erst
#    beim Deploy fest (Ingress-Host bzw. IPv6). Deshalb wird sie zur
#    Laufzeit aus der ConfigMap in /usr/share/nginx/html/config.js
#    geschrieben (Volume-Mount im Helm-Template) und im Frontend über
#    window.__SMARTPARK_CONFIG__ gelesen.
#    TODO(AP4): entsprechend im Chart mounten.

EXPOSE 8080

# Nicht-root: nginx-unprivileged-Port
RUN sed -i 's/listen\s*80;/listen 8080;/' /etc/nginx/conf.d/default.conf \
 && chown -R nginx:nginx /usr/share/nginx/html /var/cache/nginx \
 && touch /var/run/nginx.pid && chown nginx:nginx /var/run/nginx.pid

USER nginx

CMD ["nginx", "-g", "daemon off;"]
