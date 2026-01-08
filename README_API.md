# Finance Allocation Report API

FastAPI-based REST API for Finance Allocation Report Generation and Loading.

## Features

- **Health Check**: Monitor API status
- **Build Report**: Generate new reports with RepPage and RepCell records
- **Load Report**: Load existing reports or auto-generate if not exists
- **Docker Support**: Containerized deployment with Docker Compose
- **Environment Configuration**: Fully parameterized via environment variables
- **Swagger Documentation**: Interactive API docs at `/docs`

## Prerequisites

- Docker and Docker Compose
- Google Cloud Platform credentials (JSON key file)
- Access to BigQuery datasets

## Quick Start

### 1. Setup Environment

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your specific values:
- `GCP_PROJECT_ID`: Your GCP project ID
- `GCP_CREDENTIALS_PATH`: Path to your GCP credentials JSON file
- Dataset and table names (use defaults or customize)

### 2. Add GCP Credentials

Place your GCP service account JSON key file in the `credentials/` directory:

```bash
mkdir -p credentials
cp /path/to/your/fp-a-project.json credentials/
```

### 3. Build and Run with Docker Compose

```bash
# Build the Docker image
docker-compose build

# Start the service
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

### 4. Access the API

- **API Base URL**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## API Endpoints

### Health Check

```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "app_name": "Finance Allocation Report API",
  "version": "1.0.0",
  "timestamp": "2026-01-08T12:00:00.000000"
}
```

### Build Report

```http
POST /api/report/build
```

**Request Body:**
```json
{
  "my_rep_temp": "FK1",
  "my_z_block_plan": "PC-AC-PLA5-KRF",
  "my_z_block_forecast": "FC-AC-FOR1-KRF",
  "my_alt": "PLA5",
  "my_last_report_month": "M2504",
  "my_last_actual_month": "M2504"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Report built successfully",
  "data": {
    "rep_page_identifier": "PC-AC-PLA5-KRF",
    "is_newly_created": true,
    "created_at": "2026-01-08T12:00:00.000000"
  }
}
```

### Load Report

```http
POST /api/report/load
```

**Request Body:**
```json
{
  "my_rep_temp": "FK1",
  "my_z_block_plan": "PC-AC-PLA5-KRF",
  "my_z_block_forecast": "FC-AC-FOR1-KRF",
  "my_alt": "PLA5",
  "my_last_report_month": "M2504",
  "my_last_actual_month": "M2504"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Report loaded successfully with 240 records",
  "data": {
    "my_rep_page": "PC-AC-PLA5-KRF",
    "generated_at": "2026-01-08T12:00:00.000000"
  },
  "rep_cells": [
    {
      "z_number": 1001,
      "y_number1": 25001,
      "y_number2": 1,
      "y_number3": 0,
      "z_block_type": "Plan",
      "now_np": "M2504",
      "now_value": 1500000.50
    }
  ],
  "total_records": 240
}
```

## Environment Variables

### Application Settings
- `APP_NAME`: Application name (default: "Finance Allocation Report API")
- `APP_VERSION`: Application version (default: "1.0.0")
- `DEBUG`: Debug mode (default: false)

### BigQuery Settings
- `GCP_PROJECT_ID`: GCP project ID (required)
- `GCP_CREDENTIALS_PATH`: Path to GCP credentials JSON (required)

### Dataset Names
- `REPORT_DATASET_NAME`: Report data dataset (default: "Report_data")
- `REPORT_CONFIG_DATASET_NAME`: Report config dataset (default: "Report_config")
- `ALLOCATION_CONFIG_DATASET_NAME`: Allocation config dataset (default: "allocation_config")
- `ALLOC_STAGE_DATASET_NAME`: Allocation stage dataset (default: "alloc_stage")

### Table Names
- `REP_PAGE_TABLE_NAME`: RepPage table (default: "RepPage")
- `REP_TEMP_TABLE_NAME`: RepTemp table (default: "RepTemp_NativeTable")
- `REP_TEMP_BLOCK_TABLE_NAME`: RepTempBlock table (default: "RepTempBlock_NativeTable")
- `REP_CELL_TABLE_NAME`: RepCell table (default: "RepCell")
- `ALLOCATION_TO_ITEM_TABLE_NAME`: AllocationToItem table (default: "AllocationToItem_NativeTable")
- `SO_CELL_TABLE_NAME`: SOCell table (default: "so_cell_processed")

### API Settings
- `API_HOST`: API host (default: "0.0.0.0")
- `API_PORT`: API port (default: 8000)

## Development

### Run Locally (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GCP_PROJECT_ID=fp-a-project
export GCP_CREDENTIALS_PATH=/path/to/credentials.json

# Run the application
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Run Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run tests
pytest tests/
```

## Docker Commands

```bash
# Build image
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f finance-allocation-api

# Stop services
docker-compose down

# Rebuild and restart
docker-compose up -d --build

# Remove all containers and volumes
docker-compose down -v
```

## Troubleshooting

### Issue: Cannot connect to BigQuery

**Solution**: 
- Verify GCP credentials file exists and path is correct
- Check that the service account has proper BigQuery permissions
- Ensure `GCP_PROJECT_ID` matches your actual project

### Issue: Container fails to start

**Solution**:
- Check logs: `docker-compose logs finance-allocation-api`
- Verify all environment variables are set correctly
- Ensure port 8000 is not already in use

### Issue: API returns 500 errors

**Solution**:
- Check application logs for detailed error messages
- Verify BigQuery datasets and tables exist
- Ensure data format matches expected schema

## Architecture

```
FinanceAllocation/
├── api/
│   └── main.py              # FastAPI application
├── calculate/
│   └── report_runner.py     # Report generation logic
├── db/
│   └── bigquery_connector.py # BigQuery client
├── models/
│   └── report_models.py     # Data models
├── config.py                # Configuration management
├── .env.example             # Environment template
├── Dockerfile               # Docker image definition
├── docker-compose.yml       # Docker Compose configuration
└── requirements.txt         # Python dependencies
```

## API Flow

1. **Load Report Request** → Check if RepPage exists
2. **If RepPage exists** → Check if RepCell data exists
3. **If RepCell exists** → Return data
4. **If RepCell doesn't exist** → Call Build Report → Return data
5. **If RepPage doesn't exist** → Create RepPage → Call Build Report → Return data

## License

Proprietary - Internal Use Only

## Support

For issues or questions, contact the development team.
