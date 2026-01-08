# FA Allocation Report API - Docker Setup

This guide explains how to build and run the FA Allocation Report API using Docker and Docker Compose.

## Prerequisites

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- GCP credentials file at `/home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json`

## Quick Start

### 1. Create Environment File

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your specific configuration if needed.

### 2. Build Docker Image

```bash
docker-compose build
```

Or build manually:

```bash
docker build -t fa-allocation-api:latest .
```

### 3. Run with Docker Compose

```bash
docker-compose up -d
```

This will:
- Build the Docker image using Python 3.12
- Start the API container
- Expose the API on port 8000
- Mount the GCP credentials file
- Enable auto-restart on failure

### 4. Check Status

```bash
# View logs
docker-compose logs -f

# Check container status
docker-compose ps

# Check health
curl http://localhost:8000/health
```

### 5. Stop the Service

```bash
docker-compose down
```

## Docker Commands

### Build Image
```bash
docker-compose build
```

### Start Services
```bash
docker-compose up -d
```

### View Logs
```bash
docker-compose logs -f fa-allocation-api
```

### Restart Service
```bash
docker-compose restart
```

### Stop and Remove Containers
```bash
docker-compose down
```

### Rebuild and Restart
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Manual Docker Run

If you prefer to run without Docker Compose:

```bash
# Build
docker build -t fa-allocation-api:latest .

# Run
docker run -d \
  --name fa-allocation-api \
  -p 8000:8000 \
  -v $(pwd):/app \
  -v /home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json:/credentials/gcp-credentials.json:ro \
  --env-file .env \
  fa-allocation-api:latest
```

## API Access

Once running, access the API at:

- **API Base URL**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## Example API Calls

### Build Report
```bash
curl -X POST "http://localhost:8000/api/v1/reports/build" \
  -H "Content-Type: application/json" \
  -d '{
    "my_rep_temp": "KRF-L4.CDT1",
    "my_z_block_plan": "my_z_block_plan",
    "my_z_block_forecast": "my_z_block_forecast",
    "my_alt": "my_alt",
    "my_last_report_month": "M2512",
    "my_last_actual_month": "M2510"
  }'
```

### Check Job Status
```bash
curl http://localhost:8000/api/v1/jobs/{job_id}
```

## Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs fa-allocation-api

# Check if port 8000 is already in use
lsof -i :8000
```

### Permission issues with GCP credentials
```bash
# Ensure the credentials file exists and is readable
ls -la /home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json
```

### Rebuild after code changes
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Volume Mounts

The docker-compose.yml mounts:
- Current directory to `/app` (for development)
- GCP credentials to `/credentials/gcp-credentials.json` (read-only)

## Environment Variables

Key environment variables (configured in `.env`):
- `GCP_PROJECT_ID`: Google Cloud project ID
- `GCP_CREDENTIALS_PATH`: Path to GCP credentials file
- `PYTHONUNBUFFERED`: Enable Python unbuffered output
- `PYTHONPATH`: Python module search path

## Health Check

The container includes a health check that:
- Runs every 30 seconds
- Checks the `/health` endpoint
- Retries 3 times before marking unhealthy
- Waits 40 seconds before starting checks

Check health status:
```bash
docker inspect fa-allocation-api | grep -A 10 Health
```

## Production Considerations

For production deployment:

1. **Remove development volume mount** - Don't mount source code in production
2. **Use secrets management** - Don't expose credentials in environment files
3. **Configure logging** - Set up proper log aggregation
4. **Add monitoring** - Implement application monitoring
5. **Use reverse proxy** - Put nginx or similar in front of the API
6. **Enable HTTPS** - Use SSL/TLS certificates
7. **Resource limits** - Set CPU and memory limits in docker-compose.yml

Example production docker-compose.yml additions:
```yaml
services:
  fa-allocation-api:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```
