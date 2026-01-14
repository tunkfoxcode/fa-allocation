import queue
import threading
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Task:
    def __init__(self, task_id: str, task_type: str, params: Dict[str, Any]):
        self.task_id = task_id
        self.task_type = task_type
        self.params = params
        self.status = TaskStatus.PENDING
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Optional[Any] = None
        self.error: Optional[str] = None
        self.progress: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
            "progress": self.progress
        }


class TaskQueue:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        self.task_queue = queue.Queue()
        self.tasks: Dict[str, Task] = {}
        self.worker_thread: Optional[threading.Thread] = None
        self.is_running = False
        self._initialized = True

    def start_worker(self):
        """Start the background worker thread"""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            print("[INFO] Task queue worker started")

    def stop_worker(self):
        """Stop the background worker thread"""
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
            print("[INFO] Task queue worker stopped")

    def submit_task(self, task_type: str, params: Dict[str, Any]) -> str:
        """Submit a new task to the queue"""
        task_id = str(uuid.uuid4())
        task = Task(task_id=task_id, task_type=task_type, params=params)
        
        with self._lock:
            self.tasks[task_id] = task
        
        self.task_queue.put(task)
        print(f"[INFO] Task submitted: {task_id} ({task_type})")
        return task_id

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a task by ID"""
        with self._lock:
            task = self.tasks.get(task_id)
            if task:
                return task.to_dict()
        return None

    def _worker(self):
        """Background worker that processes tasks from the queue"""
        print("[INFO] Task queue worker thread started")
        
        while self.is_running:
            try:
                task = self.task_queue.get(timeout=1)
                self._process_task(task)
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ERROR] Worker error: {str(e)}")

    def _process_task(self, task: Task):
        """Process a single task"""
        print(f"[INFO] Processing task: {task.task_id} ({task.task_type})")
        
        try:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            
            if task.task_type == "build_report":
                result = self._process_build_report(task)
                task.result = result
                task.status = TaskStatus.COMPLETED
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")
            
            task.completed_at = datetime.utcnow()
            print(f"[INFO] Task completed: {task.task_id}")
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = datetime.utcnow()
            print(f"[ERROR] Task failed: {task.task_id} - {str(e)}")

    def _process_build_report(self, task: Task) -> Dict[str, Any]:
        """Process a build_report task"""
        from db.bigquery_connector import BigQueryConnector
        from calculate.report_runner import find_or_create_rep_page, build_report
        from app_config import get_settings
        
        settings = get_settings()
        params = task.params
        
        bq = BigQueryConnector(
            credentials_path=settings.GCP_CREDENTIALS_PATH,
            project_id=settings.GCP_PROJECT_ID
        )
        
        task.progress = 10
        
        rep_page, is_newly_created = find_or_create_rep_page(
            bq=bq,
            my_rep_temp=params['my_rep_temp'],
            my_z_block_plan=params['my_z_block_plan'],
            my_z_block_forecast=params['my_z_block_forecast'],
            my_alt=params['my_alt'],
            my_last_report_month=params['my_last_report_month'],
            my_last_actual_month=params['my_last_actual_month'],
            project_id=settings.GCP_PROJECT_ID,
            report_dataset_name=settings.REPORT_DATASET_NAME,
            rep_page_table_name=settings.REP_PAGE_TABLE_NAME
        )
        
        task.progress = 30
        
        rep_page_identifier = build_report(
            bq=bq,
            my_rep_page=rep_page,
            my_rep_temp=params['my_rep_temp'],
            my_last_report_month=params['my_last_report_month'],
            my_last_actual_month=params['my_last_actual_month'],
            project_id=settings.GCP_PROJECT_ID
        )
        
        task.progress = 100
        
        return {
            "rep_page_identifier": rep_page_identifier,
            "is_newly_created": is_newly_created,
            "created_at": datetime.utcnow().isoformat()
        }


task_queue_instance = TaskQueue()
