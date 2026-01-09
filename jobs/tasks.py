"""
Background tasks for job queue
"""
from db.bigquery_connector import BigQueryConnector
from calculate.report_runner import find_or_create_rep_page
from calculate.report_runner_optimized import build_report_optimized
from app_config import get_settings
from rq import get_current_job

settings = get_settings()


def build_report_task(
    my_rep_temp: str,
    my_z_block_plan: str,
    my_z_block_forecast: str,
    my_alt: str,
    my_last_report_month: str,
    my_last_actual_month: str
) -> dict:
    """
    Background task to build report
    
    Returns:
        dict with rep_page_identifier and status
    """
    job = get_current_job()
    
    try:
        # Update progress
        if job:
            job.meta['progress'] = 'Initializing BigQuery connection'
            job.save_meta()
        
        # Initialize BigQuery connector
        bq = BigQueryConnector(
            credentials_path=settings.GCP_CREDENTIALS_PATH,
            project_id=settings.GCP_PROJECT_ID
        )
        
        # Update progress
        if job:
            job.meta['progress'] = 'Finding or creating RepPage'
            job.save_meta()
        
        # Find or create RepPage
        rep_page, is_newly_created = find_or_create_rep_page(
            bq=bq,
            my_rep_temp=my_rep_temp,
            my_z_block_plan=my_z_block_plan,
            my_z_block_forecast=my_z_block_forecast,
            my_alt=my_alt,
            my_last_report_month=my_last_report_month,
            my_last_actual_month=my_last_actual_month,
            project_id=settings.GCP_PROJECT_ID,
            report_dataset_name=settings.REPORT_DATASET_NAME,
            rep_page_table_name=settings.REP_PAGE_TABLE_NAME
        )
        
        # Update progress
        if job:
            job.meta['progress'] = 'Building report with optimized algorithm'
            job.save_meta()
        
        # Build report using optimized version
        rep_page_identifier = build_report_optimized(
            bq=bq,
            my_rep_page=rep_page,
            my_rep_temp=my_rep_temp,
            my_last_report_month=my_last_report_month,
            my_last_actual_month=my_last_actual_month,
            project_id=settings.GCP_PROJECT_ID
        )
        
        # Update progress
        if job:
            job.meta['progress'] = 'Completed'
            job.save_meta()
        
        return {
            'status': 'success',
            'rep_page_identifier': rep_page_identifier,
            'is_newly_created': is_newly_created
        }
        
    except Exception as e:
        # Update progress with error
        if job:
            job.meta['progress'] = f'Failed: {str(e)}'
            job.save_meta()
        
        raise  # Re-raise to mark job as failed
