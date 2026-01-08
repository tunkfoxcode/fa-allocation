from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime
import uuid
from calculate.report_runner import build_report, load_report

app = FastAPI(
    title="FA Allocation Report API",
    description="API for running financial allocation reports",
    version="1.0.0"
)

class ReportRequest(BaseModel):
    my_rep_temp: str = Field(..., description="Report template name", example="KRF-L4.CDT1")
    my_z_block_plan: str = Field(..., description="Plan block string", example="my_z_block_plan")
    my_z_block_forecast: str = Field(..., description="Forecast block string", example="my_z_block_forecast")
    my_alt: str = Field(..., description="ALT identifier", example="my_alt")
    my_last_report_month: str = Field(..., description="Last report month (format: M2512)", example="M2512")
    my_last_actual_month: str = Field(..., description="Last actual month (format: M2510)", example="M2510")

class ReportResponse(BaseModel):
    job_id: str
    status: str
    message: str
    created_at: datetime

class JobStatus(BaseModel):
    job_id: str
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

job_store: Dict[str, JobStatus] = {}

def run_build_report_task(
    job_id: str,
    my_rep_temp: str,
    my_z_block_plan: str,
    my_z_block_forecast: str,
    my_alt: str,
    my_last_report_month: str,
    my_last_actual_month: str
):
    try:
        build_report(
            my_rep_temp=my_rep_temp,
            my_z_block_plan=my_z_block_plan,
            my_z_block_forecast=my_z_block_forecast,
            my_alt=my_alt,
            my_last_report_month=my_last_report_month,
            my_last_actual_month=my_last_actual_month
        )
        job_store[job_id].status = "completed"
        job_store[job_id].completed_at = datetime.now()
    except Exception as e:
        job_store[job_id].status = "failed"
        job_store[job_id].error = str(e)
        job_store[job_id].completed_at = datetime.now()

def run_load_report_task(
    job_id: str,
    my_rep_temp: str,
    my_z_block_plan: str,
    my_z_block_forecast: str,
    my_alt: str,
    my_last_report_month: str,
    my_last_actual_month: str
):
    try:
        load_report(
            my_rep_temp=my_rep_temp,
            my_z_block_plan=my_z_block_plan,
            my_z_block_forecast=my_z_block_forecast,
            my_alt=my_alt,
            my_last_report_month=my_last_report_month,
            my_last_actual_month=my_last_actual_month
        )
        job_store[job_id].status = "completed"
        job_store[job_id].completed_at = datetime.now()
    except Exception as e:
        job_store[job_id].status = "failed"
        job_store[job_id].error = str(e)
        job_store[job_id].completed_at = datetime.now()

@app.get("/")
async def root():
    return {
        "message": "FA Allocation Report API",
        "version": "1.0.0",
        "endpoints": {
            "build_report": "/api/v1/reports/build",
            "load_report": "/api/v1/reports/load",
            "job_status": "/api/v1/jobs/{job_id}",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

@app.post("/api/v1/reports/build", response_model=ReportResponse)
async def build_report_endpoint(
    request: ReportRequest,
    background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    created_at = datetime.now()
    
    job_store[job_id] = JobStatus(
        job_id=job_id,
        status="processing",
        created_at=created_at
    )
    
    background_tasks.add_task(
        run_build_report_task,
        job_id=job_id,
        my_rep_temp=request.my_rep_temp,
        my_z_block_plan=request.my_z_block_plan,
        my_z_block_forecast=request.my_z_block_forecast,
        my_alt=request.my_alt,
        my_last_report_month=request.my_last_report_month,
        my_last_actual_month=request.my_last_actual_month
    )
    
    return ReportResponse(
        job_id=job_id,
        status="processing",
        message="Report build job started successfully",
        created_at=created_at
    )

@app.post("/api/v1/reports/load", response_model=ReportResponse)
async def load_report_endpoint(
    request: ReportRequest,
    background_tasks: BackgroundTasks
):
    job_id = str(uuid.uuid4())
    created_at = datetime.now()
    
    job_store[job_id] = JobStatus(
        job_id=job_id,
        status="processing",
        created_at=created_at
    )
    
    background_tasks.add_task(
        run_load_report_task,
        job_id=job_id,
        my_rep_temp=request.my_rep_temp,
        my_z_block_plan=request.my_z_block_plan,
        my_z_block_forecast=request.my_z_block_forecast,
        my_alt=request.my_alt,
        my_last_report_month=request.my_last_report_month,
        my_last_actual_month=request.my_last_actual_month
    )
    
    return ReportResponse(
        job_id=job_id,
        status="processing",
        message="Report load job started successfully",
        created_at=created_at
    )

@app.get("/api/v1/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_store[job_id]

@app.delete("/api/v1/jobs/{job_id}")
async def delete_job(job_id: str):
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")
    
    del job_store[job_id]
    return {"message": f"Job {job_id} deleted successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
