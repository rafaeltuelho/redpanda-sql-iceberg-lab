# Build the React app, then serve it and the API from one Python image.
FROM node:22-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM python:3.13-slim
WORKDIR /srv
COPY app/requirements.txt app/requirements.txt
RUN pip install --no-cache-dir -r app/requirements.txt
COPY app/ app/
COPY schemas/ schemas/
COPY --from=web /web/dist web/dist
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
USER nobody
CMD ["uvicorn", "app.backend.main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers", "--forwarded-allow-ips", "*"]
