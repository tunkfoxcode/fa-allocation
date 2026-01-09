"""
Job Queue Manager using Redis Queue (RQ)
"""
import redis
from rq import Queue
from rq.job import Job
from typing import Optional, Dict, Any
from app_config import get_settings

settings = get_settings()


class JobQueueManager:
    """Manages job queue operations"""
    
    def __init__(self):
        self.redis_conn = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )
        self.queue = Queue('report_build', connection=self.redis_conn)
    
    def enqueue_build_report(
        self,
        my_rep_temp: str,
        my_z_block_plan: str,
        my_z_block_forecast: str,
        my_alt: str,
        my_last_report_month: str,
        my_last_actual_month: str
    ) -> str:
        """
        Enqueue a build report job
        
        Returns:
            job_id: Unique job identifier
        """
        from jobs.tasks import build_report_task
        
        job = self.queue.enqueue(
            build_report_task,
            my_rep_temp=my_rep_temp,
            my_z_block_plan=my_z_block_plan,
            my_z_block_forecast=my_z_block_forecast,
            my_alt=my_alt,
            my_last_report_month=my_last_report_month,
            my_last_actual_month=my_last_actual_month,
            job_timeout='30m',  # 30 minutes timeout
            result_ttl=3600,  # Keep result for 1 hour
            failure_ttl=86400  # Keep failed job info for 24 hours
        )
        
        return job.id
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status and result
        
        Returns:
            dict with status, result, error info
        """
        try:
            job = Job.fetch(job_id, connection=self.redis_conn)
            
            status_info = {
                'job_id': job_id,
                'status': job.get_status(),
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'started_at': job.started_at.isoformat() if job.started_at else None,
                'ended_at': job.ended_at.isoformat() if job.ended_at else None,
                'result': None,
                'error': None,
                'progress': None
            }
            
            # Get result if finished
            if job.is_finished:
                status_info['result'] = job.result
            
            # Get error if failed
            if job.is_failed:
                status_info['error'] = str(job.exc_info)
            
            # Get progress if available (from job meta)
            if job.meta:
                status_info['progress'] = job.meta.get('progress')
            
            return status_info
            
        except Exception as e:
            return {
                'job_id': job_id,
                'status': 'not_found',
                'error': f'Job not found: {str(e)}'
            }
    
    def get_queue_info(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'queue_name': self.queue.name,
            'queued_jobs': len(self.queue),
            'started_jobs': self.queue.started_job_registry.count,
            'finished_jobs': self.queue.finished_job_registry.count,
            'failed_jobs': self.queue.failed_job_registry.count
        }
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued or running job"""
        try:
            job = Job.fetch(job_id, connection=self.redis_conn)
            job.cancel()
            return True
        except Exception:
            return False


# Singleton instance
_queue_manager = None

def get_queue_manager() -> JobQueueManager:
    """Get or create queue manager singleton"""
    global _queue_manager
    if _queue_manager is None:
        _queue_manager = JobQueueManager()
    return _queue_manager
