import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Application settings
    APP_NAME: str = "Finance Allocation Report API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # BigQuery settings
    GCP_PROJECT_ID: str
    GCP_CREDENTIALS_PATH: str
    
    # Dataset names
    REPORT_DATASET_NAME: str = "Report_data"
    REPORT_CONFIG_DATASET_NAME: str = "Report_config"
    ALLOCATION_CONFIG_DATASET_NAME: str = "allocation_config"
    ALLOC_STAGE_DATASET_NAME: str = "alloc_stage"
    
    # Table names
    REP_PAGE_TABLE_NAME: str = "RepPage"
    REP_TEMP_TABLE_NAME: str = "RepTemp_NativeTable"
    REP_TEMP_BLOCK_TABLE_NAME: str = "RepTempBlock_NativeTable"
    REP_CELL_TABLE_NAME: str = "RepCell"
    ALLOCATION_TO_ITEM_TABLE_NAME: str = "AllocationToItem_NativeTable"
    SO_CELL_TABLE_NAME: str = "so_cell_processed"
    
    # API settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()
