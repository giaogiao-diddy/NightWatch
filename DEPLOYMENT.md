# NightWatch Docker Compose Production Deployment

## 1. Prerequisites

- Docker Engine 24+
- Docker Compose V2

## 2. Start Services

Set secrets before startup:

```powershell
$env:NW_API_AUTH_KEY = "replace-with-strong-key"
$env:GF_SECURITY_ADMIN_PASSWORD = "replace-with-strong-password"
```

Build and start:

```powershell
docker compose -f docker-compose.prod.yml up -d --build
```

## 3. Endpoints

- NightWatch API: http://localhost:8000
- MCP Server: http://localhost:9001
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## 4. Verify Monitoring

Prometheus target health:

```powershell
docker compose -f docker-compose.prod.yml ps
```

Metrics endpoint check:

```powershell
curl http://localhost:8000/metrics
```

## 5. Stop Services

```powershell
docker compose -f docker-compose.prod.yml down
```
