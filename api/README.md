# FA Allocation Report API

FastAPI application for running financial allocation reports.

## Features

- **Build Report**: Create new reports with specified parameters
- **Load Report**: Load existing reports or trigger build if not found
- **Job Tracking**: Asynchronous job processing with status tracking
- **Health Check**: Monitor API availability

## API Endpoints

### Root
- `GET /` - API information and available endpoints

### Health Check
- `GET /health` - Health check endpoint

### Reports
- `POST /api/v1/reports/build` - Build a new report
- `POST /api/v1/reports/load` - Load or build a report

### Jobs
- `GET /api/v1/jobs/{job_id}` - Get job status
- `DELETE /api/v1/jobs/{job_id}` - Delete job record

## Request Body Example

```json
{
  "my_rep_temp": "KRF-L4.CDT1",
  "my_z_block_plan": "my_z_block_plan",
  "my_z_block_forecast": "my_z_block_forecast",
  "my_alt": "my_alt",
  "my_last_report_month": "M2512",
  "my_last_actual_month": "M2510"
}
```

## Response Example

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "message": "Report build job started successfully",
  "created_at": "2026-01-08T10:20:00"
}
```

## Job Status Response

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "created_at": "2026-01-08T10:20:00",
  "completed_at": "2026-01-08T10:25:00",
  "error": null
}
```

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the API
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

## Running with Docker

See main README.md for Docker instructions.

## Interactive API Documentation

Once the API is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
