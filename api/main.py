from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
import logging
from datetime import datetime

from app_config import get_settings
from db.bigquery_connector import BigQueryConnector
from calculate.report_runner import load_report
from models.report_models import RepPage
from jobs.queue_manager import get_queue_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get settings
settings = get_settings()

# Initialize FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API for Finance Allocation Report Generation and Loading",
    docs_url="/docs",
    redoc_url="/redoc"
)


# Request/Response Models
class HealthResponse(BaseModel):
    status: str
    app_name: str
    version: str
    timestamp: str


class BuildReportRequest(BaseModel):
    my_rep_temp: str = Field(..., description="Report template identifier (e.g., 'FK1')")
    my_z_block_plan: str = Field(..., description="Plan ZBlock in format: 'Source-Pack-Scenario-Run'")
    my_z_block_forecast: str = Field(..., description="Forecast ZBlock in format: 'Source-Pack-Scenario-Run'")
    my_alt: str = Field(..., description="ALT identifier (e.g., 'PLA5')")
    my_last_report_month: str = Field(..., description="Last report month in format 'M2504' (April 2025)")
    my_last_actual_month: Optional[str] = Field(None, description="Last actual month in format 'M2504'")


class BuildReportResponse(BaseModel):
    status: str
    message: str
    job_id: str
    data: dict


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    result: Optional[dict] = None
    error: Optional[str] = None
    progress: Optional[str] = None


class QueueInfoResponse(BaseModel):
    queue_name: str
    queued_jobs: int
    started_jobs: int
    finished_jobs: int
    failed_jobs: int


class LoadReportRequest(BaseModel):
    my_rep_temp: str = Field(..., description="Report template identifier (e.g., 'FK1')")
    my_z_block_plan: str = Field(..., description="Plan ZBlock in format: 'Source-Pack-Scenario-Run'")
    my_z_block_forecast: str = Field(..., description="Forecast ZBlock in format: 'Source-Pack-Scenario-Run'")
    my_alt: str = Field(..., description="ALT identifier (e.g., 'PLA5')")
    my_last_report_month: str = Field(..., description="Last report month in format 'M2504' (April 2025)")
    my_last_actual_month: Optional[str] = Field(None, description="Last actual month in format 'M2504'")


class RepCellData(BaseModel):
    z_number: Optional[int] = None
    y_number1: Optional[int] = None
    y_number2: Optional[int] = None
    y_number3: Optional[int] = None
    my_rep_page: Optional[str] = None
    my_rep_temp_block: Optional[str] = None
    z_block_type: Optional[str] = None
    now_np: Optional[str] = None
    now_value: Optional[float] = None


class LoadReportResponse(BaseModel):
    status: str
    message: str
    data: dict
    rep_cells: List[dict]
    total_records: int


# API Endpoints
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint to verify API is running.
    """
    return HealthResponse(
        status="healthy",
        app_name=settings.APP_NAME,
        version=settings.APP_VERSION,
        timestamp=datetime.utcnow().isoformat()
    )


@app.post("/api/report/build", response_model=BuildReportResponse, tags=["Report"])
async def api_build_report(request: BuildReportRequest):
    """
    Enqueue a build report job (non-blocking).
    
    This endpoint immediately returns a job_id. The actual report building
    happens in the background. Use /api/job/{job_id} to check status.
    
    This endpoint:
    1. Validates request parameters
    2. Enqueues job to Redis queue
    3. Returns job_id immediately
    
    Background job will:
    1. Create or find RepPage entry
    2. Query RepTemp and RepTempBlock configurations
    3. Generate filter combinations (Cartesian product)
    4. Query SOCell data for Plan, Actual, and Forecast (optimized batch queries)
    5. Create RepCell records for each combination and period
    
    Returns:
        BuildReportResponse with job_id for status tracking
    """
    try:
        logger.info(f"Enqueueing build report job for: {request.my_rep_temp}, {request.my_z_block_plan}")
        
        # Get queue manager
        queue_manager = get_queue_manager()
        
        # Enqueue job
        job_id = queue_manager.enqueue_build_report(
            my_rep_temp=request.my_rep_temp,
            my_z_block_plan=request.my_z_block_plan,
            my_z_block_forecast=request.my_z_block_forecast,
            my_alt=request.my_alt,
            my_last_report_month=request.my_last_report_month,
            my_last_actual_month=request.my_last_actual_month or request.my_last_report_month
        )
        
        logger.info(f"Job enqueued successfully: {job_id}")
        
        return BuildReportResponse(
            status="queued",
            message="Report build job enqueued successfully. Use job_id to check status.",
            job_id=job_id,
            data={
                "my_rep_temp": request.my_rep_temp,
                "my_z_block_plan": request.my_z_block_plan,
                "enqueued_at": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error enqueueing build report job: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enqueue build report job: {str(e)}"
        )


@app.post("/api/report/load", response_model=LoadReportResponse, tags=["Report"])
async def api_load_report(request: LoadReportRequest):
    """
    Load report data. Main entry point for users.
    
    This endpoint checks if a report exists for the given parameters.
    If the report exists (RepPage found with RepCell data), it returns the array of RepCell data.
    If not, it automatically calls build_report to generate the report first, then returns the RepCell data.
    
    Returns:
        LoadReportResponse with array of RepCell records
    """
    try:
        logger.info(f"Loading report for: {request.my_rep_temp}, {request.my_z_block_plan}")
        
        # Call load_report function
        rep_cells = load_report(
            my_rep_temp=request.my_rep_temp,
            my_z_block_plan=request.my_z_block_plan,
            my_z_block_forecast=request.my_z_block_forecast,
            my_alt=request.my_alt,
            my_last_report_month=request.my_last_report_month,
            my_last_actual_month=request.my_last_actual_month or request.my_last_report_month
        )
        
        # Convert RepCell objects to dictionaries
        rep_cells_data = [cell.to_bigquery_dict() for cell in rep_cells]
        
        logger.info(f"Report loaded successfully: {len(rep_cells)} records")
        
        return LoadReportResponse(
            status="success",
            message=f"Report loaded successfully with {len(rep_cells)} records",
            data={
                "my_rep_page": request.my_z_block_plan,
                "generated_at": datetime.utcnow().isoformat()
            },
            rep_cells=rep_cells_data,
            total_records=len(rep_cells)
        )
        
    except Exception as e:
        logger.error(f"Error loading report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load report: {str(e)}"
        )


# Exception handlers
@app.get("/api/job/{job_id}", response_model=JobStatusResponse, tags=["Job"])
async def get_job_status(job_id: str):
    """
    Get status of a background job.
    
    Returns job status, progress, result, or error information.
    
    Status values:
    - queued: Job is waiting in queue
    - started: Job is currently running
    - finished: Job completed successfully
    - failed: Job failed with error
    - not_found: Job ID not found
    """
    try:
        queue_manager = get_queue_manager()
        status_info = queue_manager.get_job_status(job_id)
        
        return JobStatusResponse(**status_info)
        
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get job status: {str(e)}"
        )


@app.get("/api/queue/info", response_model=QueueInfoResponse, tags=["Job"])
async def get_queue_info():
    """
    Get queue statistics.
    
    Returns information about jobs in different states.
    """
    try:
        queue_manager = get_queue_manager()
        queue_info = queue_manager.get_queue_info()
        
        return QueueInfoResponse(**queue_info)
        
    except Exception as e:
        logger.error(f"Error getting queue info: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue info: {str(e)}"
        )


@app.delete("/api/job/{job_id}", tags=["Job"])
async def cancel_job(job_id: str):
    """
    Cancel a queued or running job.
    """
    try:
        queue_manager = get_queue_manager()
        success = queue_manager.cancel_job(job_id)
        
        if success:
            return {"status": "success", "message": f"Job {job_id} cancelled"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found or cannot be cancelled"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel job: {str(e)}"
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "message": exc.detail,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "status": "error",
            "message": "Internal server error",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
