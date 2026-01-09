"""
In-memory job queue manager using asyncio
Simple, no external dependencies needed
"""
import asyncio
import uuid
from datetime import datetime
from typing import Dict, Optional, Any
from enum import Enum
from dataclasses import dataclass, field
import threading


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    job_id: str
    task_name: str
    params: Dict[str, Any]
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    progress: str = "Queued"


class InMemoryJobQueue:
    """
    Simple in-memory job queue with single worker thread
    """
    
    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.queue: asyncio.Queue = asyncio.Queue()
        self.worker_task: Optional[asyncio.Task] = None
        self.is_running = False
        self._lock = threading.Lock()
    
    def enqueue_job(self, task_name: str, **params) -> str:
        """
        Add job to queue
        
        Returns:
            job_id: Unique job identifier
        """
        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id,
            task_name=task_name,
            params=params
        )
        
        with self._lock:
            self.jobs[job_id] = job
        
        # Add to queue (non-blocking)
        try:
            self.queue.put_nowait(job_id)
        except asyncio.QueueFull:
            job.status = JobStatus.FAILED
            job.error = "Queue is full"
        
        return job_id
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status and details"""
        with self._lock:
            job = self.jobs.get(job_id)
        
        if not job:
            return {
                'job_id': job_id,
                'status': 'not_found',
                'error': 'Job not found'
            }
        
        return {
            'job_id': job.job_id,
            'status': job.status.value,
            'created_at': job.created_at.isoformat() if job.created_at else None,
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'result': job.result,
            'error': job.error,
            'progress': job.progress
        }
    
    def get_queue_info(self) -> Dict[str, Any]:
        """Get queue statistics"""
        with self._lock:
            queued = sum(1 for j in self.jobs.values() if j.status == JobStatus.QUEUED)
            running = sum(1 for j in self.jobs.values() if j.status == JobStatus.RUNNING)
            completed = sum(1 for j in self.jobs.values() if j.status == JobStatus.COMPLETED)
            failed = sum(1 for j in self.jobs.values() if j.status == JobStatus.FAILED)
        
        return {
            'queue_name': 'in_memory',
            'queued_jobs': queued,
            'running_jobs': running,
            'completed_jobs': completed,
            'failed_jobs': failed,
            'total_jobs': len(self.jobs)
        }
    
    async def process_job(self, job_id: str):
        """Process a single job"""
        with self._lock:
            job = self.jobs.get(job_id)
        
        if not job:
            return
        
        try:
            # Update status to running
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            job.progress = "Starting job"
            
            # Import task function
            if job.task_name == "build_report":
                from jobs.tasks import build_report_task
                
                # Update progress
                job.progress = "Building report..."
                
                # Execute task
                result = build_report_task(**job.params)
                
                # Update job with result
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                job.result = result
                job.progress = "Completed"
                
            else:
                raise ValueError(f"Unknown task: {job.task_name}")
                
        except Exception as e:
            # Handle error
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error = str(e)
            job.progress = f"Failed: {str(e)}"
            print(f"[ERROR] Job {job_id} failed: {str(e)}")
    
    async def worker(self):
        """Background worker that processes jobs one at a time"""
        print("[INFO] In-memory job worker started")
        
        while self.is_running:
            try:
                # Wait for job (with timeout to check is_running)
                job_id = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                print(f"[INFO] Processing job: {job_id}")
                await self.process_job(job_id)
                print(f"[INFO] Job completed: {job_id}")
                
            except asyncio.TimeoutError:
                # No job available, continue loop
                continue
            except Exception as e:
                print(f"[ERROR] Worker error: {str(e)}")
    
    async def start_worker(self):
        """Start the background worker"""
        if not self.is_running:
            self.is_running = True
            self.worker_task = asyncio.create_task(self.worker())
            print("[INFO] Job worker started")
    
    async def stop_worker(self):
        """Stop the background worker"""
        self.is_running = False
        if self.worker_task:
            await self.worker_task
            print("[INFO] Job worker stopped")
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job (only if queued)"""
        with self._lock:
            job = self.jobs.get(job_id)
            if job and job.status == JobStatus.QUEUED:
                job.status = JobStatus.FAILED
                job.error = "Cancelled by user"
                job.completed_at = datetime.utcnow()
                return True
        return False


# Global singleton instance
_job_queue: Optional[InMemoryJobQueue] = None


def get_job_queue() -> InMemoryJobQueue:
    """Get or create job queue singleton"""
    global _job_queue
    if _job_queue is None:
        _job_queue = InMemoryJobQueue()
    return _job_queue
