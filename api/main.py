from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import logging
from datetime import datetime

from app_config import get_settings
from db.bigquery_connector import BigQueryConnector
from calculate.report_runner import load_report, build_report
from models.report_models import RepPage
from api.task_queue import task_queue_instance

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

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Start task queue worker on startup
@app.on_event("startup")
async def startup_event():
    task_queue_instance.start_worker()
    logger.info("Task queue worker started")

@app.on_event("shutdown")
async def shutdown_event():
    task_queue_instance.stop_worker()
    logger.info("Task queue worker stopped")


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
    data: dict


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


class TaskStatusResponse(BaseModel):
    status: str
    message: str
    task: Optional[dict] = None


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
    Submit a report build task to the queue.
    
    This endpoint submits a task to build a report asynchronously.
    The task will:
    1. Create or find RepPage entry
    2. Query RepTemp and RepTempBlock configurations
    3. Generate filter combinations (Cartesian product)
    4. Query SOCell data for Plan, Actual, and Forecast
    5. Create RepCell records for each combination and period
    
    Returns:
        BuildReportResponse with task_id to track progress
    """
    try:
        logger.info(f"Submitting build report task for: {request.my_rep_temp}, {request.my_z_block_plan}")
        
        # Submit task to queue
        task_id = task_queue_instance.submit_task(
            task_type="build_report",
            params={
                "my_rep_temp": request.my_rep_temp,
                "my_z_block_plan": request.my_z_block_plan,
                "my_z_block_forecast": request.my_z_block_forecast,
                "my_alt": request.my_alt,
                "my_last_report_month": request.my_last_report_month,
                "my_last_actual_month": request.my_last_actual_month or request.my_last_report_month
            }
        )
        
        logger.info(f"Task submitted successfully: {task_id}")
        
        return BuildReportResponse(
            status="success",
            message="Report build task submitted successfully",
            data={
                "task_id": task_id,
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error submitting build report task: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit build report task: {str(e)}"
        )


@app.get("/api/task/{task_id}", response_model=TaskStatusResponse, tags=["Task"])
async def get_task_status(task_id: str):
    """
    Get the status of a task by its ID.
    
    This endpoint allows you to check the progress and status of a build_report task.
    
    Returns:
        TaskStatusResponse with task details including status, progress, result, or error
    """
    try:
        task_status = task_queue_instance.get_task_status(task_id)
        
        if task_status is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task not found: {task_id}"
            )
        
        return TaskStatusResponse(
            status="success",
            message=f"Task status retrieved successfully",
            task=task_status
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving task status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve task status: {str(e)}"
        )


@app.post("/api/report/load", response_model=LoadReportResponse, tags=["Report"])
async def api_load_report(request: LoadReportRequest):
    """
    Load report data. Main entry point for users.
    
    This endpoint checks if a report exists for the given parameters.
    If the report exists (RepPage found with RepCell data), it returns the array of RepCell data.
    If not, it submits a build task to the queue and returns empty data with task_id.
    
    Returns:
        LoadReportResponse with array of RepCell records or task_id if building
    """
    try:
        logger.info(f"Loading report for: {request.my_rep_temp}, {request.my_z_block_plan}")
        
        # Call load_report function - returns (rep_cells, task_id, message)
        rep_cells, task_id, message = load_report(
            my_rep_temp=request.my_rep_temp,
            my_z_block_plan=request.my_z_block_plan,
            my_z_block_forecast=request.my_z_block_forecast,
            my_alt=request.my_alt,
            my_last_report_month=request.my_last_report_month,
            my_last_actual_month=request.my_last_actual_month or request.my_last_report_month
        )
        
        # Convert RepCell objects to dictionaries
        rep_cells_data = [cell.to_bigquery_dict() for cell in rep_cells]
        
        # Prepare response data
        response_data = {
            "my_rep_page": request.my_z_block_plan,
            "generated_at": datetime.utcnow().isoformat()
        }
        
        # If task_id exists, add it to response data
        if task_id:
            response_data["task_id"] = task_id
            response_data["status"] = "building"
            logger.info(f"Report build task submitted: {task_id}")
        else:
            logger.info(f"Report loaded successfully: {len(rep_cells)} records")
        
        return LoadReportResponse(
            status="success",
            message=message,
            data=response_data,
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
