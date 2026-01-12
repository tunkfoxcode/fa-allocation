from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
import logging
from datetime import datetime

from app_config import get_settings
from db.bigquery_connector import BigQueryConnector
from calculate.report_runner import load_report, build_report
from models.report_models import RepPage

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
    Build a new report by generating RepPage and RepCell records.
    
    This endpoint:
    1. Creates or finds RepPage entry
    2. Queries RepTemp and RepTempBlock configurations
    3. Generates filter combinations (Cartesian product)
    4. Queries SOCell data for Plan, Actual, and Forecast
    5. Creates RepCell records for each combination and period
    
    Returns:
        BuildReportResponse with RepPage identifier and metadata
    """
    try:
        logger.info(f"Building report for: {request.my_rep_temp}, {request.my_z_block_plan}")
        
        # Initialize BigQuery connector
        bq = BigQueryConnector(
            credentials_path=settings.GCP_CREDENTIALS_PATH,
            project_id=settings.GCP_PROJECT_ID
        )
        
        # Import here to avoid circular dependency
        from calculate.report_runner import find_or_create_rep_page
        
        # Find or create RepPage
        rep_page, is_newly_created = find_or_create_rep_page(
            bq=bq,
            my_rep_temp=request.my_rep_temp,
            my_z_block_plan=request.my_z_block_plan,
            my_z_block_forecast=request.my_z_block_forecast,
            my_alt=request.my_alt,
            my_last_report_month=request.my_last_report_month,
            my_last_actual_month=request.my_last_actual_month or request.my_last_report_month,
            project_id=settings.GCP_PROJECT_ID,
            report_dataset_name=settings.REPORT_DATASET_NAME,
            rep_page_table_name=settings.REP_PAGE_TABLE_NAME
        )
        
        # Build report
        rep_page_identifier = build_report(
            bq=bq,
            my_rep_page=rep_page,
            my_rep_temp=request.my_rep_temp,
            my_last_report_month=request.my_last_report_month,
            my_last_actual_month=request.my_last_actual_month or request.my_last_report_month,
            project_id=settings.GCP_PROJECT_ID
        )
        
        logger.info(f"Report built successfully: {rep_page_identifier}")
        
        return BuildReportResponse(
            status="success",
            message="Report built successfully",
            data={
                "rep_page_identifier": rep_page_identifier,
                "is_newly_created": is_newly_created,
                "created_at": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Error building report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build report: {str(e)}"
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
