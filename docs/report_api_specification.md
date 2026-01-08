# Report API Specification

## Overview
This document describes the API endpoints for the Report Generation and Loading system. The system provides two main operations:
1. **Load Report** - Retrieve existing report data or trigger report generation if not exists
2. **Build Report** - Generate a new report and create RepPage entry

---

## API Endpoints

### 1. Load Report
**Endpoint:** `POST /api/report/load`

**Description:** 
Main entry point for users. This endpoint checks if a report exists for the given parameters. If the report exists (MyRepPage found), it returns the array of RepCell data. If not, it automatically calls BuildReport to generate the report first, then returns the RepCell data.

**Request Body:**
```json
{
  "my_rep_temp": "string",           // Report template identifier (e.g., "FK1")
  "my_z_block_plan": "string",       // Plan ZBlock in format: "Source-Pack-Scenario-Run"
  "my_z_block_forecast": "string",   // Forecast ZBlock in format: "Source-Pack-Scenario-Run"
  "my_alt": "string",                // ALT identifier (e.g., "PLA5")
  "my_last_report_month": "string",  // Last report month in format "M2504" (April 2025)
  "my_rep_page": "string" (optional) // If provided, directly load from this RepPage
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "my_rep_page": "string",         // RepPage identifier
    "rep_cells": [                   // Array of RepCell objects
      {
        "z_number": "integer",
        "y_number1": "integer",
        "y_number2": "integer",
        "y_number3": "integer",
        "my_rep_page": "string",
        "my_rep_temp_block": "string",
        "z_block_type": "string",    // "Plan" or "ActualForecast"
        "now_y_block_kr_item_code_kr1": "string",
        "now_y_block_kr_item_code_kr2": "string",
        // ... other KR fields
        "now_y_block_cdt_cdt1": "string",
        "now_y_block_cdt_cdt2": "string",
        // ... other filter fields
        "now_np": "string",          // Period in format "M2504"
        "now_value": "float"         // Numeric value
      }
    ],
    "total_records": "integer",
    "generated_at": "datetime"       // ISO 8601 format
  }
}
```

**Error Response:**
```json
{
  "status": "error",
  "error": {
    "code": "string",
    "message": "string",
    "details": "object"
  }
}
```

**Workflow:**
1. If `my_rep_page` is provided → Query RepCell directly
2. If `my_rep_page` is NOT provided:
   - Call `find_or_create_rep_page()` to check if RepPage exists
   - If RepPage doesn't exist → Call `build_report()` to generate
   - Query RepCell using the RepPage identifier
3. Return array of RepCell records

---

### 2. Build Report
**Endpoint:** `POST /api/report/build`

**Description:**
Generates a new report by processing RepTemp and RepTempBlock configurations, querying SOCell data, and creating RepCell records. This endpoint is typically called internally by LoadReport, but can be called directly for manual report generation.

**Request Body:**
```json
{
  "my_rep_temp": "string",           // Report template identifier (e.g., "FK1")
  "my_z_block_plan": "string",       // Plan ZBlock in format: "Source-Pack-Scenario-Run"
  "my_z_block_forecast": "string",   // Forecast ZBlock in format: "Source-Pack-Scenario-Run"
  "my_alt": "string",                // ALT identifier (e.g., "PLA5")
  "my_last_report_month": "string"   // Last report month in format "M2504" (April 2025)
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "my_rep_page": "string",         // Generated RepPage identifier
    "z_number": "integer",
    "y_number1": "integer",
    "rep_cells_created": "integer",  // Total number of RepCell records created
    "processing_time_seconds": "float",
    "created_at": "datetime"         // ISO 8601 format
  }
}
```

**Error Response:**
```json
{
  "status": "error",
  "error": {
    "code": "string",
    "message": "string",
    "details": "object"
  }
}
```

**Processing Steps:**
1. Find or create RepPage entry
2. Query RepTemp configuration
3. Query RepTempBlock configurations
4. For each RepTempBlock:
   - Build filter item combinations (Cartesian product)
   - For each combination and period (L = 0 to 5):
     - Query SOCell for Plan, Actual, and Forecast
     - Create RepCell records with appropriate Z_BLOCK_TYPE
5. Return RepPage identifier

---

## Data Models

### RepPage
```typescript
interface RepPage {
  z_number: number;
  y_number1: number;
  z_block_zblock_plan_source: string;
  z_block_zblock_plan_pack: string;
  z_block_zblock_plan_scenario: string;
  z_block_zblock_plan_run: string;
  z_block_forecast_source: string;
  z_block_forecast_pack: string;
  z_block_forecast_scenario: string;
  z_block_forecast_run: string;
  now_zblock2_alt: string;
  time_x_block_last_acttual_month: string;
  time_x_block_created_at: string;  // ISO 8601
}
```

### RepCell
```typescript
interface RepCell {
  z_number: number;
  y_number1: number;
  y_number2: number;
  y_number3: number;
  my_rep_page: string;
  my_rep_temp_block: string;
  z_block_type: "Plan" | "ActualForecast";
  
  // KR Fields
  now_y_block_kr_item_code_kr1?: string;
  now_y_block_kr_item_code_kr2?: string;
  now_y_block_kr_item_code_kr3?: string;
  now_y_block_kr_item_code_kr4?: string;
  now_y_block_kr_item_code_kr5?: string;
  now_y_block_kr_item_code_kr6?: string;
  now_y_block_kr_item_code_kr7?: string;
  now_y_block_kr_item_code_kr8?: string;
  now_y_block_kr_item_name?: string;
  
  // Filter Fields
  now_y_block_cdt_cdt1?: string;
  now_y_block_cdt_cdt2?: string;
  now_y_block_cdt_cdt3?: string;
  now_y_block_cdt_cdt4?: string;
  now_y_block_ptnow_pt1?: string;
  now_y_block_ptnow_pt2?: string;
  now_y_block_ptnow_duration?: string;
  now_y_block_ptprev_pt1?: string;
  now_y_block_ptprev_pt2?: string;
  now_y_block_ptprev_duration?: string;
  now_y_block_ptfix_owntype?: string;
  now_y_block_ptfix_aitype?: string;
  now_y_block_ptsub_cty1?: string;
  now_y_block_ptsub_cty2?: string;
  now_y_block_ptsub_ostype?: string;
  now_y_block_funnel_fu1?: string;
  now_y_block_funnel_fu2?: string;
  now_y_block_channel_ch?: string;
  now_y_block_employee_egt1?: string;
  now_y_block_employee_egt2?: string;
  now_y_block_employee_egt3?: string;
  now_y_block_employee_egt4?: string;
  now_y_block_hr_hr1?: string;
  now_y_block_hr_hr2?: string;
  now_y_block_hr_hr3?: string;
  now_y_block_sec?: string;
  now_y_block_period_mx?: string;
  now_y_block_period_dx?: string;
  now_y_block_period_ppc?: string;
  now_y_block_period_np?: string;
  now_y_block_le_le1?: string;
  now_y_block_le_le2?: string;
  now_y_block_unit?: string;
  now_y_block_td_bu?: string;
  
  // Period and Value
  now_np: string;      // Format: "M2504"
  now_value: number;   // Numeric value
}
```

---

## Error Codes

| Code | Description |
|------|-------------|
| `INVALID_PARAMETERS` | Missing or invalid request parameters |
| `REP_TEMP_NOT_FOUND` | RepTemp configuration not found |
| `REP_TEMP_BLOCK_NOT_FOUND` | RepTempBlock configuration not found |
| `SOCELL_QUERY_ERROR` | Error querying SOCell data |
| `DATABASE_ERROR` | Database operation failed |
| `REPORT_GENERATION_FAILED` | Report generation process failed |
| `REP_PAGE_NOT_FOUND` | RepPage not found for given parameters |

---

## Usage Examples

### Example 1: Load Report (First Time - Triggers Build)
```javascript
// Request
POST /api/report/load
{
  "my_rep_temp": "FK1",
  "my_z_block_plan": "PC-AC-PLA5-KRF",
  "my_z_block_forecast": "FC-AC-FOR1-KRF",
  "my_alt": "PLA5",
  "my_last_report_month": "M2504"
}

// Response
{
  "status": "success",
  "data": {
    "my_rep_page": "PC-AC-PLA5-KRF",
    "rep_cells": [
      {
        "z_number": 1001,
        "y_number1": 25001,
        "y_number2": 1,
        "y_number3": 0,
        "z_block_type": "Plan",
        "now_np": "M2504",
        "now_value": 1500000.50
      },
      // ... more records
    ],
    "total_records": 240,
    "generated_at": "2026-01-08T15:30:00Z"
  }
}
```

### Example 2: Load Report (Existing RepPage)
```javascript
// Request
POST /api/report/load
{
  "my_rep_page": "PC-AC-PLA5-KRF"
}

// Response
{
  "status": "success",
  "data": {
    "my_rep_page": "PC-AC-PLA5-KRF",
    "rep_cells": [ /* array of RepCell */ ],
    "total_records": 240,
    "generated_at": "2026-01-08T15:30:00Z"
  }
}
```

### Example 3: Build Report (Manual Trigger)
```javascript
// Request
POST /api/report/build
{
  "my_rep_temp": "FK1",
  "my_z_block_plan": "PC-AC-PLA5-KRF",
  "my_z_block_forecast": "FC-AC-FOR1-KRF",
  "my_alt": "PLA5",
  "my_last_report_month": "M2504"
}

// Response
{
  "status": "success",
  "data": {
    "my_rep_page": "PC-AC-PLA5-KRF",
    "z_number": 1001,
    "y_number1": 25001,
    "rep_cells_created": 240,
    "processing_time_seconds": 12.5,
    "created_at": "2026-01-08T15:30:00Z"
  }
}
```

---

## Notes for Frontend Integration

1. **Primary Endpoint**: Use `POST /api/report/load` as the main entry point
2. **Caching**: Consider caching RepPage identifiers to speed up subsequent loads
3. **Loading States**: Report generation can take 10-30 seconds depending on data volume
4. **Period Format**: All periods use format `M{YY}{MM}` (e.g., M2504 = April 2025)
5. **Z_BLOCK_TYPE**: 
   - "Plan" = Plan data from ZBlockPlan
   - "ActualForecast" = Actual data (if period <= LastActualMonth) or Forecast data (if period > LastActualMonth)
6. **Array Size**: Expect 40-500 RepCell records per report depending on filter combinations and periods
7. **Null Values**: Many NOW_Y_BLOCK fields can be null - handle appropriately in UI

---

## Status
- **Version**: 1.0
- **Last Updated**: 2026-01-08
- **Status**: Draft - Backend implementation in progress
- **Expected Completion**: TBD

---

## Contact
For questions or clarifications, please contact the Backend Development Team.
