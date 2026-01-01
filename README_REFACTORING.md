# Finance Allocation - Refactored Structure

## Overview
The codebase has been refactored following the **Single Responsibility Principle**. All code from `main.py` has been split into separate modules based on their responsibilities.

## New Directory Structure

```
FinanceAllocation/
├── db/
│   ├── __init__.py
│   └── bigquery_connector.py          # BigQuery database connection logic
├── models/
│   ├── __init__.py
│   ├── allocation_models.py           # AllocationALT, AllocationToItem, AllocationByType, AllocationByKR
│   └── so_cell_model.py               # SoCell model
├── config/
│   ├── __init__.py
│   └── field_mappings.py              # Field mapping constants (YBLOCK_FIELD_MAPPING, PREV_YBLOCK_FIELD_MAPPING)
├── queries/
│   ├── __init__.py
│   └── query_builder.py               # Query building functions
├── services/
│   ├── __init__.py
│   ├── so_cell_factory.py             # SoCell creation factory functions
│   └── allocation_service.py          # Business logic for allocation calculations
├── utils/
│   ├── __init__.py
│   └── period_utils.py                # Period calculation utilities
├── main.py                            # Refactored main entry point
├── main_original_backup.py            # Original main.py backup
└── requirements.txt
```

## Module Responsibilities

### 1. `db/bigquery_connector.py`
- **Responsibility**: Database connection and operations
- **Classes**: `BigQueryConnector`
- **Methods**: 
  - `execute_query()` - Execute SQL queries
  - `insert_row()` - Insert data into BigQuery
  - `list_datasets()`, `list_tables()`, `get_table_schema()` - Utility methods

### 2. `models/allocation_models.py`
- **Responsibility**: Data models for allocation tables
- **Classes**: 
  - `AllocationALT` - Allocation ALT table model
  - `AllocationToItem` - Allocation ToItem table model
  - `AllocationByType` - Allocation ByType table model
  - `AllocationByKR` - Allocation ByKR table model
- **Features**: Factory methods for creating instances from BigQuery rows/DataFrames

### 3. `models/so_cell_model.py`
- **Responsibility**: Data model for SoCell table
- **Classes**: `SoCell`
- **Features**: Factory methods for creating instances from BigQuery rows/DataFrames

### 4. `config/field_mappings.py`
- **Responsibility**: Configuration constants for field mappings
- **Constants**:
  - `YBLOCK_FIELD_MAPPING` - Maps AllocationByType fields to SoCell NowYBlock fields
  - `PREV_YBLOCK_FIELD_MAPPING` - Maps SoCell NowYBlock to PrevYBlock fields

### 5. `queries/query_builder.py`
- **Responsibility**: Dynamic SQL query construction
- **Functions**:
  - `build_so_cell_query()` - Build query for SoCell based on AllocationByType
  - `build_so_cell_by_kr_query()` - Build query for SoCell based on AllocationByKR
  - `build_so_cell_prev_query()` - Build query for SoCell PrevYBlock matching

### 6. `services/so_cell_factory.py`
- **Responsibility**: Factory functions for creating SoCell instances
- **Functions**:
  - `create_socell_from_yblocks()` - Create SoCell from two YBlocks
  - `create_socell_for_offset()` - Create SoCell for offset calculations

### 7. `services/allocation_service.py`
- **Responsibility**: Business logic for allocation calculations
- **Functions**:
  - `calculate_offset()` - Handle offset allocation calculations

### 8. `utils/period_utils.py`
- **Responsibility**: Period/date calculation utilities
- **Functions**:
  - `add_period_strings()` - Add two period strings (e.g., "M2907" + "MP04")
  - `add_period_with_offset()` - Add integer offset to period string

### 9. `main.py`
- **Responsibility**: Application entry point and orchestration
- **Features**: Coordinates all modules to execute the allocation workflow

## Benefits of Refactoring

1. **Maintainability**: Each module has a clear, single responsibility
2. **Testability**: Individual modules can be tested independently
3. **Reusability**: Functions and classes can be reused across different parts of the application
4. **Readability**: Code is organized logically, making it easier to understand
5. **Scalability**: New features can be added without modifying existing modules

## Migration Notes

- Original `main.py` has been backed up as `main_original_backup.py`
- All functionality remains the same, just organized differently
- Import statements have been updated to use the new module structure

## Usage

Run the application the same way as before:

```bash
python main.py
```

All imports are handled automatically through the new module structure.
