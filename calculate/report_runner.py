from db.bigquery_connector import BigQueryConnector
from models.report_models import RepPage, RepTemp, RepTempBlock, RepCell
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from itertools import product
from app_config import get_settings

settings = get_settings()


def build_filter_and_kr_data(
        bq: BigQueryConnector,
        project_id: str,
        my_rep_temp_block: RepTempBlock
) -> Tuple[Dict[str, List[str]], Dict[str, str], List[Dict[str, str]]]:
    """
    Build filter item map, KR type full, and filter combinations from RepTempBlock.

    This function performs:
    - Step 70: Collect filter types and query AllocationToItem
    - Step 80: Collect KR-related fields
    - Step 100: Create Cartesian product of filter items

    Args:
        bq: BigQuery connector instance
        project_id: GCP project ID
        my_rep_temp_block: RepTempBlock instance containing filter and KR fields

    Returns:
        Tuple containing:
        - my_filter_item_map: Dictionary mapping field names to lists of filter items
        - my_kr_type_full: Dictionary of KR-related fields (non-None values only)
        - filter_combinations: List of all filter item combinations
    """
    # Step70 MyFilterType - Collect filter types and query AllocationToItem
    field_map = {
        'NOW_Y_BLOCK_CDT_CDT1': my_rep_temp_block.now_y_block_cdt_cdt1,
        'NOW_Y_BLOCK_CDT_CDT2': my_rep_temp_block.now_y_block_cdt_cdt2,
        'NOW_Y_BLOCK_CDT_CDT3': my_rep_temp_block.now_y_block_cdt_cdt3,
        'NOW_Y_BLOCK_CDT_CDT4': my_rep_temp_block.now_y_block_cdt_cdt4,
        'NOW_Y_BLOCK_PTNow_PT1': my_rep_temp_block.now_y_block_ptnow_pt1,
        'NOW_Y_BLOCK_PTNow_PT2': my_rep_temp_block.now_y_block_ptnow_pt2,
        'NOW_Y_BLOCK_PTNow_Duration': my_rep_temp_block.now_y_block_ptnow_duration,
        'NOW_Y_BLOCK_PTPrev_PT1': my_rep_temp_block.now_y_block_ptprev_pt1,
        'NOW_Y_BLOCK_PTPrev_PT2': my_rep_temp_block.now_y_block_ptprev_pt2,
        'NOW_Y_BLOCK_PTPrev_Duration': my_rep_temp_block.now_y_block_ptprev_duration,
        'NOW_Y_BLOCK_PTFix_OwnType': my_rep_temp_block.now_y_block_ptfix_owntype,
        'NOW_Y_BLOCK_PTFix_AIType': my_rep_temp_block.now_y_block_ptfix_aitype,
        'NOW_Y_BLOCK_PTSub_CTY1': my_rep_temp_block.now_y_block_ptsub_cty1,
        'NOW_Y_BLOCK_PTSub_CTY2': my_rep_temp_block.now_y_block_ptsub_cty2,
        'NOW_Y_BLOCK_PTSub_OSType': my_rep_temp_block.now_y_block_ptsub_ostype,
        'NOW_Y_BLOCK_Funnel_FU1': my_rep_temp_block.now_y_block_funnel_fu1,
        'NOW_Y_BLOCK_Funnel_FU2': my_rep_temp_block.now_y_block_funnel_fu2,
        'NOW_Y_BLOCK_Channel_CH': my_rep_temp_block.now_y_block_channel_ch,
        'NOW_Y_BLOCK_Employee_EGT1': my_rep_temp_block.now_y_block_employee_egt1,
        'NOW_Y_BLOCK_Employee_EGT2': my_rep_temp_block.now_y_block_employee_egt2,
        'NOW_Y_BLOCK_Employee_EGT3': my_rep_temp_block.now_y_block_employee_egt3,
        'NOW_Y_BLOCK_Employee_EGT4': my_rep_temp_block.now_y_block_employee_egt4,
        'NOW_Y_BLOCK_HR_HR1': my_rep_temp_block.now_y_block_hr_hr1,
        'NOW_Y_BLOCK_HR_HR2': my_rep_temp_block.now_y_block_hr_hr2,
        'NOW_Y_BLOCK_HR_HR3': my_rep_temp_block.now_y_block_hr_hr3,
        'NOW_Y_BLOCK_SEC': my_rep_temp_block.now_y_block_sec,
        'NOW_Y_BLOCK_Period_MX': my_rep_temp_block.now_y_block_period_mx,
        'NOW_Y_BLOCK_Period_DX': my_rep_temp_block.now_y_block_period_dx,
        'NOW_Y_BLOCK_Period_PPC': my_rep_temp_block.now_y_block_period_ppc,
        'NOW_Y_BLOCK_Period_NP': my_rep_temp_block.now_y_block_period_np,
        'NOW_Y_BLOCK_LE_LE1': my_rep_temp_block.now_y_block_le_le1,
        'NOW_Y_BLOCK_LE_LE2': my_rep_temp_block.now_y_block_le_le2,
        'NOW_Y_BLOCK_UNIT': my_rep_temp_block.now_y_block_unit,
        # 'NOW_Y_BLOCK_TD_BU': my_rep_temp_block.now_y_block_td_bu
    }

    field_map = {k: v for k, v in field_map.items() if v is not None}
    print(f"[INFO][Step 70] Field map with non-None values: {field_map}")

    # Build mapping from field_name to to_type
    field_to_type_map = {}
    for field_name, field_value in field_map.items():
        if '-' in field_value:
            parts = field_value.rsplit('-', 1)
            if len(parts) == 2 and parts[1].isdigit():
                to_type = parts[0]
            else:
                to_type = field_value
        else:
            to_type = field_value
        field_to_type_map[field_name] = to_type

    # Collect all unique to_type values
    to_type_list = list(set(field_to_type_map.values()))

    # Query all filter items at once with IN clause
    my_filter_item_map = {}
    if to_type_list:
        to_type_str = "', '".join(to_type_list)
        query_filter_items = f"""
        SELECT TO_Y_BLOCK_ToType, TO_Y_BLOCK_ToItem 
        FROM `{project_id}.{settings.ALLOCATION_CONFIG_DATASET_NAME}.{settings.ALLOCATION_TO_ITEM_TABLE_NAME}` 
        WHERE TO_Y_BLOCK_ToType IN ('{to_type_str}')
        """

        filter_items_df = bq.execute_query(query_filter_items)

        # Group results by TO_Y_BLOCK_ToType
        to_type_items_map = {}
        for _, row in filter_items_df.iterrows():
            to_type = row['TO_Y_BLOCK_ToType']
            to_item = row['TO_Y_BLOCK_ToItem']
            if to_type not in to_type_items_map:
                to_type_items_map[to_type] = []
            to_type_items_map[to_type].append(to_item)

        # Map back to field names
        for field_name, to_type in field_to_type_map.items():
            if to_type in to_type_items_map:
                my_filter_item_map[field_name] = to_type_items_map[to_type]
                print(f"[INFO][Step 70] {field_name} ({to_type}): Found {len(to_type_items_map[to_type])} items")

    print(f"[INFO][Step 70] MyFilterItemMap: {my_filter_item_map}")

    # Step80 MyKRTypeFull - Collect KR-related fields
    my_kr_type_full = {
        'NOW_Y_BLOCK_FNF_FNF': my_rep_temp_block.now_y_block_fnf_fnf,
        'NOW_Y_BLOCK_KR_Item_Code_KR1': my_rep_temp_block.now_y_block_kr_item_code_kr1,
        'NOW_Y_BLOCK_KR_Item_Code_KR2': my_rep_temp_block.now_y_block_kr_item_code_kr2,
        'NOW_Y_BLOCK_KR_Item_Code_KR3': my_rep_temp_block.now_y_block_kr_item_code_kr3,
        'NOW_Y_BLOCK_KR_Item_Code_KR4': my_rep_temp_block.now_y_block_kr_item_code_kr4,
        'NOW_Y_BLOCK_KR_Item_Code_KR5': my_rep_temp_block.now_y_block_kr_item_code_kr5,
        'NOW_Y_BLOCK_KR_Item_Code_KR6': my_rep_temp_block.now_y_block_kr_item_code_kr6,
        'NOW_Y_BLOCK_KR_Item_Code_KR7': my_rep_temp_block.now_y_block_kr_item_code_kr7,
        'NOW_Y_BLOCK_KR_Item_Code_KR8': my_rep_temp_block.now_y_block_kr_item_code_kr8,
        'NOW_Y_BLOCK_KR_Item_Name': my_rep_temp_block.now_y_block_kr_item_name
    }

    my_kr_type_full = {k: v for k, v in my_kr_type_full.items() if v is not None}
    print(f"[INFO][Step 80] MyKRTypeFull: {my_kr_type_full}")

    # Step100 Create Cartesian product of filter items
    if my_filter_item_map:
        field_names = list(my_filter_item_map.keys())
        field_values_lists = list(my_filter_item_map.values())

        filter_combinations = []
        for combination in product(*field_values_lists):
            combination_map = {}
            for i, field_name in enumerate(field_names):
                combination_map[field_name] = combination[i]
            filter_combinations.append(combination_map)

        print(f"[INFO][Step 100] Created {len(filter_combinations)} filter combinations")
        print(f"[INFO][Step 100] First 3 combinations: {filter_combinations[:3]}")
    else:
        filter_combinations = []
        print(f"[INFO][Step 100] No filter items to combine")

    return my_filter_item_map, my_kr_type_full, filter_combinations


def calculate_x_period(my_last_report_month: str, l: int) -> str:
    """
    Calculate MyXPeriod by subtracting L months from MyLastReportMonth.

    Args:
        my_last_report_month: Last report month in format M{YY}{MM} (e.g., "M2504" for April 2025)
        l: Number of months to subtract

    Returns:
        MyXPeriod in format M{YY}{MM}
    """
    year = int(my_last_report_month[1:3])
    month = int(my_last_report_month[3:5])

    total_months = year * 12 + month - l
    new_year = total_months // 12
    new_month = total_months % 12

    if new_month == 0:
        new_month = 12
        new_year -= 1

    my_x_period = f"M{new_year:02d}{new_month:02d}"
    return my_x_period


def query_so_cell_data(
        bq: BigQueryConnector,
        project_id: str,
        my_rep_page,
        my_kr_type_full: Dict[str, str],
        my_filter_item: Dict[str, str],
        x_period_list: List[str],
        my_alt: str
) -> Tuple[Dict[str, any], Dict[str, any], Dict[str, any]]:
    """
    Query SOCell data for Plan, Actual, and Forecast scenarios for all periods.

    Args:
        bq: BigQuery connector instance
        project_id: GCP project ID
        my_rep_page: RepPage instance containing ZBlock information
        my_kr_type_full: Dictionary of KR-related fields
        my_filter_item: Dictionary of filter items
        x_period_list: List of period strings to query
        my_alt: ALT identifier for NOW_ZBlock2_ALT filter

    Returns:
        Tuple containing:
        - plan_data: Dictionary mapping period to plan value
        - actual_data: Dictionary mapping period to actual value
        - forecast_data: Dictionary mapping period to forecast value
    """
    # Field mappings (shared across all queries)
    kr_field_mapping = {
        'NOW_Y_BLOCK_FNF_FNF': 'now_y_block_fnf_fnf',
        'NOW_Y_BLOCK_KR_Item_Code_KR1': 'now_y_block_kr_item_code_kr1',
        'NOW_Y_BLOCK_KR_Item_Code_KR2': 'now_y_block_kr_item_code_kr2',
        'NOW_Y_BLOCK_KR_Item_Code_KR3': 'now_y_block_kr_item_code_kr3',
        'NOW_Y_BLOCK_KR_Item_Code_KR4': 'now_y_block_kr_item_code_kr4',
        'NOW_Y_BLOCK_KR_Item_Code_KR5': 'now_y_block_kr_item_code_kr5',
        'NOW_Y_BLOCK_KR_Item_Code_KR6': 'now_y_block_kr_item_code_kr6',
        'NOW_Y_BLOCK_KR_Item_Code_KR7': 'now_y_block_kr_item_code_kr7',
        'NOW_Y_BLOCK_KR_Item_Code_KR8': 'now_y_block_kr_item_code_kr8',
        'NOW_Y_BLOCK_KR_Item_Name': 'now_y_block_kr_item_name'
    }

    filter_field_mapping = {
        'NOW_Y_BLOCK_CDT_CDT1': 'now_y_block_cdt_cdt1',
        'NOW_Y_BLOCK_CDT_CDT2': 'now_y_block_cdt_cdt2',
        'NOW_Y_BLOCK_CDT_CDT3': 'now_y_block_cdt_cdt3',
        'NOW_Y_BLOCK_CDT_CDT4': 'now_y_block_cdt_cdt4',
        'NOW_Y_BLOCK_PTNow_PT1': 'now_y_block_ptnow_pt1',
        'NOW_Y_BLOCK_PTNow_PT2': 'now_y_block_ptnow_pt2',
        'NOW_Y_BLOCK_PTNow_Duration': 'now_y_block_ptnow_duration',
        'NOW_Y_BLOCK_PTPrev_PT1': 'now_y_block_ptprev_pt1',
        'NOW_Y_BLOCK_PTPrev_PT2': 'now_y_block_ptprev_pt2',
        'NOW_Y_BLOCK_PTPrev_Duration': 'now_y_block_ptprev_duration',
        'NOW_Y_BLOCK_PTFix_OwnType': 'now_y_block_ptfix_owntype',
        'NOW_Y_BLOCK_PTFix_AIType': 'now_y_block_ptfix_aitype',
        'NOW_Y_BLOCK_PTSub_CTY1': 'now_y_block_ptsub_cty1',
        'NOW_Y_BLOCK_PTSub_CTY2': 'now_y_block_ptsub_cty2',
        'NOW_Y_BLOCK_PTSub_OSType': 'now_y_block_ptsub_ostype',
        'NOW_Y_BLOCK_Funnel_FU1': 'now_y_block_funnel_fu1',
        'NOW_Y_BLOCK_Funnel_FU2': 'now_y_block_funnel_fu2',
        'NOW_Y_BLOCK_Channel_CH': 'now_y_block_channel_ch',
        'NOW_Y_BLOCK_Employee_EGT1': 'now_y_block_employee_egt1',
        'NOW_Y_BLOCK_Employee_EGT2': 'now_y_block_employee_egt2',
        'NOW_Y_BLOCK_Employee_EGT3': 'now_y_block_employee_egt3',
        'NOW_Y_BLOCK_Employee_EGT4': 'now_y_block_employee_egt4',
        'NOW_Y_BLOCK_HR_HR1': 'now_y_block_hr_hr1',
        'NOW_Y_BLOCK_HR_HR2': 'now_y_block_hr_hr2',
        'NOW_Y_BLOCK_HR_HR3': 'now_y_block_hr_hr3',
        'NOW_Y_BLOCK_SEC': 'now_y_block_sec',
        'NOW_Y_BLOCK_Period_MX': 'now_y_block_period_mx',
        'NOW_Y_BLOCK_Period_DX': 'now_y_block_period_dx',
        'NOW_Y_BLOCK_Period_PPC': 'now_y_block_period_ppc',
        'NOW_Y_BLOCK_Period_NP': 'now_y_block_period_np',
        'NOW_Y_BLOCK_LE_LE1': 'now_y_block_le_le1',
        'NOW_Y_BLOCK_LE_LE2': 'now_y_block_le_le2',
        'NOW_Y_BLOCK_UNIT': 'now_y_block_unit',
        'NOW_Y_BLOCK_TD_BU': 'now_y_block_td_bu'
    }

    periods_str = "', '".join(x_period_list)

    # Step160 Query Plan data for ALL periods at once
    z_block_plan_source = my_rep_page.z_block_zblock_plan_source
    z_block_plan_pack = my_rep_page.z_block_zblock_plan_pack
    z_block_plan_scenario = my_rep_page.z_block_zblock_plan_scenario
    z_block_plan_run = my_rep_page.z_block_zblock_plan_run

    where_conditions = []
    if z_block_plan_source:
        where_conditions.append(f"z_block_zblock1_source = '{z_block_plan_source}'")
    if z_block_plan_pack:
        where_conditions.append(f"z_block_zblock1_pack = '{z_block_plan_pack}'")
    if z_block_plan_scenario:
        where_conditions.append(f"z_block_zblock1_scenario = '{z_block_plan_scenario}'")
    if z_block_plan_run:
        where_conditions.append(f"z_block_zblock1_run = '{z_block_plan_run}'")

    for kr_field, kr_value in my_kr_type_full.items():
        if kr_field in kr_field_mapping:
            so_cell_field = kr_field_mapping[kr_field]
            where_conditions.append(f"{so_cell_field} = '{kr_value}'")

    for filter_field, so_cell_field in filter_field_mapping.items():
        if filter_field in my_filter_item:
            # If the filter has a specific value, add an equality condition
            filter_value = my_filter_item[filter_field]
            where_conditions.append(f"{so_cell_field} = '{filter_value}'")
        else:
            # If the filter is not in my_filter_item, it was NULL in RepTempBlock.
            # So, we filter for NULL values in the so_cell table.
            where_conditions.append(f"{so_cell_field} IS NULL")

    where_conditions.append(f"now_np IN ('{periods_str}')")
    where_conditions.append(f"NOW_ZBlock2_ALT = '{my_alt}'")

    query_so_cell = f"""
    SELECT now_np, now_value 
    FROM `{project_id}.{settings.ALLOC_STAGE_DATASET_NAME}.{settings.SO_CELL_TABLE_NAME}` 
    WHERE {' AND '.join(where_conditions)}
    ORDER BY uploaded_at DESC
    """

    so_cell_df = bq.execute_query(query_so_cell)
    plan_data = {row['now_np']: row['now_value'] for _, row in so_cell_df.iterrows()}
    print(f"[INFO][Step 160] Queried Plan data for {len(plan_data)} periods")

    # Step170 Query Actual data for ALL periods at once
    where_conditions_actual = ["z_block_zblock1_source = 'ACTUAL'"]

    for kr_field, kr_value in my_kr_type_full.items():
        if kr_field in kr_field_mapping:
            so_cell_field = kr_field_mapping[kr_field]
            where_conditions_actual.append(f"{so_cell_field} = '{kr_value}'")

    for filter_field, so_cell_field in filter_field_mapping.items():
        if filter_field in my_filter_item:
            filter_value = my_filter_item[filter_field]
            where_conditions_actual.append(f"{so_cell_field} = '{filter_value}'")
        else:
            where_conditions_actual.append(f"{so_cell_field} IS NULL")

    where_conditions_actual.append(f"now_np IN ('{periods_str}')")
    where_conditions_actual.append(f"NOW_ZBlock2_ALT = '{my_alt}'")

    query_so_cell_actual = f"""
    SELECT now_np, now_value 
    FROM `{project_id}.{settings.ALLOC_STAGE_DATASET_NAME}.{settings.SO_CELL_TABLE_NAME}` 
    WHERE {' AND '.join(where_conditions_actual)}
    ORDER BY uploaded_at DESC
    """

    so_cell_actual_df = bq.execute_query(query_so_cell_actual)
    actual_data = {row['now_np']: row['now_value'] for _, row in so_cell_actual_df.iterrows()}
    print(f"[INFO][Step 170] Queried Actual data for {len(actual_data)} periods")

    # Step180 Query Forecast data for ALL periods at once
    z_block_forecast_source = my_rep_page.z_block_forecast_source
    z_block_forecast_pack = my_rep_page.z_block_forecast_pack
    z_block_forecast_scenario = my_rep_page.z_block_forecast_scenario
    z_block_forecast_run = my_rep_page.z_block_forecast_run

    where_conditions_forecast = []
    if z_block_forecast_source:
        where_conditions_forecast.append(f"z_block_zblock1_source = '{z_block_forecast_source}'")
    if z_block_forecast_pack:
        where_conditions_forecast.append(f"z_block_zblock1_pack = '{z_block_forecast_pack}'")
    if z_block_forecast_scenario:
        where_conditions_forecast.append(f"z_block_zblock1_scenario = '{z_block_forecast_scenario}'")
    if z_block_forecast_run:
        where_conditions_forecast.append(f"z_block_zblock1_run = '{z_block_forecast_run}'")

    for kr_field, kr_value in my_kr_type_full.items():
        if kr_field in kr_field_mapping:
            so_cell_field = kr_field_mapping[kr_field]
            where_conditions_forecast.append(f"{so_cell_field} = '{kr_value}'")

    for filter_field, so_cell_field in filter_field_mapping.items():
        if filter_field in my_filter_item:
            filter_value = my_filter_item[filter_field]
            where_conditions_forecast.append(f"{so_cell_field} = '{filter_value}'")
        else:
            where_conditions_forecast.append(f"{so_cell_field} IS NULL")

    where_conditions_forecast.append(f"now_np IN ('{periods_str}')")
    where_conditions_forecast.append(f"NOW_ZBlock2_ALT = '{my_alt}'")

    query_so_cell_forecast = f"""
    SELECT now_np, now_value 
    FROM `{project_id}.{settings.ALLOC_STAGE_DATASET_NAME}.{settings.SO_CELL_TABLE_NAME}` 
    WHERE {' AND '.join(where_conditions_forecast)}
    ORDER BY uploaded_at DESC
    """

    so_cell_forecast_df = bq.execute_query(query_so_cell_forecast)
    forecast_data = {row['now_np']: row['now_value'] for _, row in so_cell_forecast_df.iterrows()}
    print(f"[INFO][Step 180] Queried Forecast data for {len(forecast_data)} periods")

    return plan_data, actual_data, forecast_data


def calculate_y_number1(
        bq: BigQueryConnector,
        project_id: str,
        z_block_plan_source: str,
        z_block_plan_pack: str,
        z_block_plan_scenario: str,
        z_block_plan_run: str,
        z_block_forecast_source: str,
        z_block_forecast_pack: str,
        z_block_forecast_scenario: str,
        z_block_forecast_run: str,
        my_alt: str
) -> int:
    """
    Calculate YNumber1 = ZBlockPlan.YNumber * 1000000 + ZBlockForecast.YNumber * 1000 + MyALT.YNumber

    Raises:
        Exception: If unable to query or calculate YNumber1

    Returns:
        int: Calculated YNumber1
    """
    try:
        # Query ZBlockPlan.YNumber
        query_zblock_plan = f"""
        SELECT YNumber FROM `{project_id}.{settings.REPORT_CONFIG_DATASET_NAME}.ZBlock1_NativeTable` 
        WHERE Z_BLOCK_ZBlockPlan_Source = '{z_block_plan_source}'
        AND Z_BLOCK_ZBlockPlan_Pack = '{z_block_plan_pack}'
        AND Z_BLOCK_ZBlockPlan_Scenario = '{z_block_plan_scenario}'
        AND Z_BLOCK_ZBlockPlan_Run = '{z_block_plan_run}'
        LIMIT 1
        """
        print(f"[INFO] Querying ZBlockPlan YNumber...")
        zblock_plan_df = bq.execute_query(query_zblock_plan)

        if len(zblock_plan_df) == 0:
            raise Exception(
                f"ZBlockPlan not found: {z_block_plan_source}-{z_block_plan_pack}-{z_block_plan_scenario}-{z_block_plan_run}")

        zblock_plan_ynumber = int(zblock_plan_df.iloc[0]['YNumber'])
        print(f"[INFO] ZBlockPlan YNumber: {zblock_plan_ynumber}")

        # Query ZBlockForecast.YNumber
        query_zblock_forecast = f"""
        SELECT YNumber FROM `{project_id}.{settings.REPORT_CONFIG_DATASET_NAME}.ZBlock1_NativeTable` 
        WHERE Z_BLOCK_ZBlockForecast_Source = '{z_block_forecast_source}'
        AND Z_BLOCK_ZBlockForecast_Pack = '{z_block_forecast_pack}'
        AND Z_BLOCK_ZBlockForecast_Scenario = '{z_block_forecast_scenario}'
        AND Z_BLOCK_ZBlockForecast_Run = '{z_block_forecast_run}'
        LIMIT 1
        """
        print(f"[INFO] Querying ZBlockForecast YNumber...")
        zblock_forecast_df = bq.execute_query(query_zblock_forecast)

        if len(zblock_forecast_df) == 0:
            raise Exception(
                f"ZBlockForecast not found: {z_block_forecast_source}-{z_block_forecast_pack}-{z_block_forecast_scenario}-{z_block_forecast_run}")

        zblock_forecast_ynumber = int(zblock_forecast_df.iloc[0]['YNumber'])
        print(f"[INFO] ZBlockForecast YNumber: {zblock_forecast_ynumber}")

        # Query MyALT.YNumber
        query_alt = f"""
        SELECT YNumber FROM `{project_id}.{settings.REPORT_CONFIG_DATASET_NAME}.ZBlock1_NativeTable` 
        WHERE NOW_ZBlock2_ALT = '{my_alt}'
        LIMIT 1
        """
        print(f"[INFO] Querying MyALT YNumber for: {my_alt}...")
        alt_df = bq.execute_query(query_alt)

        if len(alt_df) == 0:
            raise Exception(f"MyALT not found: {my_alt}")

        alt_ynumber = int(alt_df.iloc[0]['YNumber'])
        print(f"[INFO] MyALT YNumber: {alt_ynumber}")

        # Calculate YNumber1
        y_number1 = zblock_plan_ynumber * 1000000 + zblock_forecast_ynumber * 1000 + alt_ynumber
        print(
            f"[INFO] Calculated YNumber1: {zblock_plan_ynumber} * 1000000 + {zblock_forecast_ynumber} * 1000 + {alt_ynumber} = {y_number1}")

        return y_number1

    except Exception as e:
        error_msg = f"Failed to calculate YNumber1: {str(e)}"
        print(f"[ERROR] {error_msg}")
        raise Exception(error_msg)


def find_or_create_rep_page(
        bq: BigQueryConnector,
        my_rep_temp: str,
        my_z_block_plan: str,
        my_z_block_forecast: str,
        my_alt: str,
        my_last_report_month: str,
        my_last_actual_month: str,
        project_id: str,
        report_dataset_name: str,
        rep_page_table_name: str
) -> tuple[RepPage, bool]:
    """
    Find or create RepPage record in BigQuery

    Args:
        bq: BigQueryConnector instance
        my_rep_temp: Report template name
        my_z_block_plan: Plan block string (format: "source-pack-scenario-run")
        my_z_block_forecast: Forecast block string (format: "source-pack-scenario-run")
        my_alt: ALT identifier
        my_last_report_month: Last report month
        my_last_actual_month: Last actual month
        project_id: BigQuery project ID
        report_dataset_name: Report dataset name
        rep_page_table_name: RepPage table name

    Returns:
        tuple[RepPage, bool]: (RepPage instance, is_newly_created flag)
    """
    plan_parts = my_z_block_plan.split("-")
    z_block_plan_source = plan_parts[0] if len(plan_parts) > 0 else None
    z_block_plan_pack = plan_parts[1] if len(plan_parts) > 1 else None
    z_block_plan_scenario = plan_parts[2] if len(plan_parts) > 2 else None
    z_block_plan_run = plan_parts[3] if len(plan_parts) > 3 else None

    forecast_parts = my_z_block_forecast.split("-")
    z_block_forecast_source = forecast_parts[0] if len(forecast_parts) > 0 else None
    z_block_forecast_pack = forecast_parts[1] if len(forecast_parts) > 1 else None
    z_block_forecast_scenario = forecast_parts[2] if len(forecast_parts) > 2 else None
    z_block_forecast_run = forecast_parts[3] if len(forecast_parts) > 3 else None

    # Build query conditions dynamically, skip None values
    conditions = [f"MyRepTemp = '{my_rep_temp}'"]

    if z_block_plan_source is not None:
        conditions.append(f"Z_BLOCK_ZBlockPlan_Source = '{z_block_plan_source}'")
    if z_block_plan_pack is not None:
        conditions.append(f"Z_BLOCK_ZBlockPlan_Pack = '{z_block_plan_pack}'")
    if z_block_plan_scenario is not None:
        conditions.append(f"Z_BLOCK_ZBlockPlan_Scenario = '{z_block_plan_scenario}'")
    if z_block_plan_run is not None:
        conditions.append(f"Z_BLOCK_ZBlockPlan_Run = '{z_block_plan_run}'")
    if z_block_forecast_source is not None:
        conditions.append(f"ZBlockForecast_Source = '{z_block_forecast_source}'")
    if z_block_forecast_pack is not None:
        conditions.append(f"ZBlockForecast_Pack = '{z_block_forecast_pack}'")
    if z_block_forecast_scenario is not None:
        conditions.append(f"ZBlockForecast_Scenario = '{z_block_forecast_scenario}'")
    if z_block_forecast_run is not None:
        conditions.append(f"ZBlockForecast_Run = '{z_block_forecast_run}'")

    conditions.append(f"NOW_ZBlock2_ALT = '{my_alt}'")

    query_find_rep_page = f"""
    SELECT * 
    FROM `{project_id}.{report_dataset_name}.{rep_page_table_name}` 
    WHERE {' AND '.join(conditions)}
    LIMIT 1
    """

    rep_page_df = bq.execute_query(query_find_rep_page)
    rep_page_items = RepPage.from_dataframe(rep_page_df)

    if len(rep_page_items) > 0:
        my_rep_page = rep_page_items[0]
        print(f"[INFO][Step 20] Found existing RepPage: {my_rep_page}")
        return my_rep_page, False
    else:
        my_rep_page = RepPage(
            y_number1=None,
            my_rep_temp=my_rep_temp,
            z_block_zblock_plan_source=z_block_plan_source,
            z_block_zblock_plan_pack=z_block_plan_pack,
            z_block_zblock_plan_scenario=z_block_plan_scenario,
            z_block_zblock_plan_run=z_block_plan_run,
            z_block_forecast_source=z_block_forecast_source,
            z_block_forecast_pack=z_block_forecast_pack,
            z_block_forecast_scenario=z_block_forecast_scenario,
            z_block_forecast_run=z_block_forecast_run,
            now_zblock2_alt=my_alt,
            time_x_block_last_report_month=my_last_report_month,
            time_x_block_last_acttual_month=my_last_actual_month,
            time_x_block_upload_at=datetime.now()
        )

        # Step21 RepPage.YNumber1 = ZBlockPlan.YNumber * 1000000 + ZBlockForecast.YNumber * 1000 + MyALT.YNumber
        # Calculate YNumber1 using dedicated function (will raise exception if fails)
        my_rep_page.y_number1 = calculate_y_number1(
            bq=bq,
            project_id=project_id,
            z_block_plan_source=z_block_plan_source,
            z_block_plan_pack=z_block_plan_pack,
            z_block_plan_scenario=z_block_plan_scenario,
            z_block_plan_run=z_block_plan_run,
            z_block_forecast_source=z_block_forecast_source,
            z_block_forecast_pack=z_block_forecast_pack,
            z_block_forecast_scenario=z_block_forecast_scenario,
            z_block_forecast_run=z_block_forecast_run,
            my_alt=my_alt
        )

        success = bq.insert_row(
            dataset_id=report_dataset_name,
            table_id=rep_page_table_name,
            row_data=my_rep_page.to_bigquery_dict()
        )

        if success:
            print(f"[INFO][Step 20] Created new RepPage: {my_rep_page}")
            return my_rep_page, True
        else:
            print(f"[ERROR][Step 20] Failed to create RepPage")
            raise Exception("Failed to create RepPage in BigQuery")


def load_report(
        my_rep_temp: str,
        my_z_block_plan: str,
        my_z_block_forecast: str,
        my_alt: str,
        my_last_report_month: str,
        my_last_actual_month: str = None
) -> Tuple[List[RepCell], Optional[str], str]:
    """
    Main entry point for loading report data.

    This function:
    1. Finds or creates RepPage
    2. If newly created or RepCell data doesn't exist:
       - Submits build task to queue
       - Returns empty list with task_id and message
    3. If RepCell data exists:
       - Loads and returns RepCell data

    Args:
        my_rep_temp: Report template name
        my_z_block_plan: Plan block string
        my_z_block_forecast: Forecast block string
        my_alt: ALT identifier
        my_last_report_month: Last report month
        my_last_actual_month: Last actual month (optional)

    Returns:
        Tuple[List[RepCell], Optional[str], str]: (rep_cells, task_id, message)
        - If data exists: (rep_cells, None, "success")
        - If building: ([], task_id, "building")
    """
    print("[INFO] Starting load_report")

    bq = BigQueryConnector(
        credentials_path=settings.GCP_CREDENTIALS_PATH,
        project_id=settings.GCP_PROJECT_ID
    )

    print(f"[INFO] Processing report for: {my_rep_temp}, {my_z_block_plan}, {my_z_block_forecast}")

    # Step 1: Find or create RepPage
    existing_rep_page, is_newly_created = find_or_create_rep_page(
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

    print(f"[INFO] Using RepPage: {existing_rep_page} (newly_created={is_newly_created})")

    # Step 2: Check if RepCell data exists
    need_to_build = False

    if is_newly_created:
        # RepPage just created - definitely need to build report
        print(f"[INFO] RepPage newly created, need to build report")
        need_to_build = True

    # Step 3: If need to build, submit task to queue and return empty with task_id
    if need_to_build:
        from api.task_queue import task_queue_instance

        print(f"[INFO] Submitting build report task to queue...")
        task_id = task_queue_instance.submit_task(
            task_type="build_report",
            params={
                "my_rep_temp": my_rep_temp,
                "my_z_block_plan": my_z_block_plan,
                "my_z_block_forecast": my_z_block_forecast,
                "my_alt": my_alt,
                "my_last_report_month": my_last_report_month,
                "my_last_actual_month": my_last_actual_month or my_last_report_month
            }
        )

        print(f"[INFO] Build task submitted with task_id: {task_id}")
        return [], task_id, "Report is being built. Please check task status."

    # Step 4: Calculate MyXPeriodLoadList for 6 recent months (M from 0 to 5)
    my_x_period_load_list = []
    for m in range(6):
        my_x_period_load = calculate_x_period(my_last_report_month, m)
        my_x_period_load_list.append(my_x_period_load)

    print(f"[INFO] Calculated MyXPeriodLoadList: {my_x_period_load_list}")

    # Step 5: Load and return RepCell data using YNumber1, MyRepTempBlock, and NOW_NP IN clause
    y_number_1 = existing_rep_page.y_number1
    my_rep_temp_value = existing_rep_page.my_rep_temp
    print(f"[INFO] Loading RepCell data for YNumber1: {y_number_1}, MyRepTempBlock: {my_rep_temp_value}")

    # Build IN clause for periods
    period_in_clause = "', '".join(my_x_period_load_list)

    query_rep_cell = f"""
    SELECT * 
    FROM `{settings.GCP_PROJECT_ID}.{settings.REPORT_DATASET_NAME}.{settings.REP_CELL_TABLE_NAME}` 
    WHERE YNumber1 = {y_number_1}
    AND MyRepTempBlock = '{my_rep_temp_value}'
    AND NOW_NP IN ('{period_in_clause}')
    ORDER BY YNumber2, YNumber3, Z_BLOCK_TYPE
    """

    rep_cell_df = bq.execute_query(query_rep_cell)

    if len(rep_cell_df) == 0:
        print(f"[WARN] No RepCell data found for YNumber1: {y_number_1}, MyRepTempBlock: {my_rep_temp_value}")
        return [], None, "No data found"

    rep_cells = RepCell.from_dataframe(rep_cell_df)
    print(f"[INFO] Successfully loaded {len(rep_cells)} RepCell records")
    return rep_cells, None, "Data loaded successfully"


def build_report(
        bq: BigQueryConnector,
        my_rep_page: RepPage,
        my_rep_temp: str,
        my_last_report_month: str,
        my_last_actual_month: str,
        project_id: str
) -> str:
    # Step30A
    my_last_report_month = "M3012"
    """
    Build a new report by generating RepCell records.

    This function:
    1. Queries RepTemp and RepTempBlock configurations
    2. Generates filter combinations (Cartesian product)
    3. Queries SOCell data for Plan, Actual, and Forecast
    4. Creates RepCell records for each combination and period

    Args:
        bq: BigQueryConnector instance
        my_rep_page: RepPage instance (already created)
        my_rep_temp: Report template name
        my_last_report_month: Last report month
        my_last_actual_month: Last actual month
        project_id: BigQuery project ID

    Returns:
        str: RepPage identifier
    """
    rep_page_identifier = f"{my_rep_page.z_block_zblock_plan_source}-{my_rep_page.z_block_zblock_plan_pack}-{my_rep_page.z_block_zblock_plan_scenario}-{my_rep_page.z_block_zblock_plan_run}"
    print("[INFO] Starting build_report")

    print(f"[INFO][Step 20] Using RepPage: {my_rep_page}")

    # Step30 Query from RepTemp (REP_TEMP_TYPE = MyRepTemp)
    query_rep_temp = f"""
    SELECT * 
    FROM `{project_id}.{settings.REPORT_CONFIG_DATASET_NAME}.{settings.REP_TEMP_TABLE_NAME}` 
    WHERE REP_TEMP_TYPE = '{my_rep_temp}'
    """

    rep_temp_df = bq.execute_query(query_rep_temp)
    rep_temp_items = RepTemp.from_dataframe(rep_temp_df)

    print(f"[INFO][Step 30] Found {len(rep_temp_items)} RepTemp records for type '{my_rep_temp}'")

    my_rep_temp_item = rep_temp_items[0]

    # Step31 MyZNumber = RepTemp.ZNumber
    my_z_number = my_rep_temp_item.z_number

    # Step33 MyYNumber1 = RepPage.YNumber1
    my_y_number_1 = my_rep_page.y_number1

    # Step40 Query from RepTempBlock: (MyRepTemp) -> MyRepTempBlockList

    query_rep_temp_block = f"""
    SELECT * 
    FROM `{project_id}.{settings.REPORT_CONFIG_DATASET_NAME}.{settings.REP_TEMP_BLOCK_TABLE_NAME}` 
    WHERE MyRepTemp = '{my_rep_temp}'
    ORDER BY ynumber2
    """

    rep_temp_block_df = bq.execute_query(query_rep_temp_block)
    my_rep_temp_block_list = RepTempBlock.from_dataframe(rep_temp_block_df)

    print(f"[INFO][Step 40] Found {len(my_rep_temp_block_list)} RepTempBlock records for FK1 '{my_rep_temp}'")

    # Step50 Foreach  MyRepTempBlock in MyRepTempBlockList (YNumber2 Increasing)
    for my_rep_temp_block in my_rep_temp_block_list:
        print(f"[INFO][Step 40] Processing RepTempBlock: {my_rep_temp_block}")

        # TODO: Add further processing logic for each RepTempBlock item
        # Step60 MyYNumber2 = MyRepTempBlock.YNumber2
        my_y_number_2 = my_rep_temp_block.y_number2

        # Step70-100: Build filter item map, KR type full, and filter combinations
        my_filter_item_map, my_kr_type_full, filter_combinations = build_filter_and_kr_data(
            bq, project_id, my_rep_temp_block
        )

        # Field mappings (defined once, reused for all filter items)
        kr_field_map = {
            'NOW_Y_BLOCK_FNF_FNF': 'now_y_block_fnf_fnf',
            'NOW_Y_BLOCK_KR_Item_Code_KR1': 'now_y_block_kr_item_code_kr1',
            'NOW_Y_BLOCK_KR_Item_Code_KR2': 'now_y_block_kr_item_code_kr2',
            'NOW_Y_BLOCK_KR_Item_Code_KR3': 'now_y_block_kr_item_code_kr3',
            'NOW_Y_BLOCK_KR_Item_Code_KR4': 'now_y_block_kr_item_code_kr4',
            'NOW_Y_BLOCK_KR_Item_Code_KR5': 'now_y_block_kr_item_code_kr5',
            'NOW_Y_BLOCK_KR_Item_Code_KR6': 'now_y_block_kr_item_code_kr6',
            'NOW_Y_BLOCK_KR_Item_Code_KR7': 'now_y_block_kr_item_code_kr7',
            'NOW_Y_BLOCK_KR_Item_Code_KR8': 'now_y_block_kr_item_code_kr8',
            'NOW_Y_BLOCK_KR_Item_Name': 'now_y_block_kr_item_name'
        }

        filter_field_map = {
            'NOW_Y_BLOCK_CDT_CDT1': 'now_y_block_cdt_cdt1',
            'NOW_Y_BLOCK_CDT_CDT2': 'now_y_block_cdt_cdt2',
            'NOW_Y_BLOCK_CDT_CDT3': 'now_y_block_cdt_cdt3',
            'NOW_Y_BLOCK_CDT_CDT4': 'now_y_block_cdt_cdt4',
            'NOW_Y_BLOCK_PTNow_PT1': 'now_y_block_ptnow_pt1',
            'NOW_Y_BLOCK_PTNow_PT2': 'now_y_block_ptnow_pt2',
            'NOW_Y_BLOCK_PTNow_Duration': 'now_y_block_ptnow_duration',
            'NOW_Y_BLOCK_PTPrev_PT1': 'now_y_block_ptprev_pt1',
            'NOW_Y_BLOCK_PTPrev_PT2': 'now_y_block_ptprev_pt2',
            'NOW_Y_BLOCK_PTPrev_Duration': 'now_y_block_ptprev_duration',
            'NOW_Y_BLOCK_PTFix_OwnType': 'now_y_block_ptfix_owntype',
            'NOW_Y_BLOCK_PTFix_AIType': 'now_y_block_ptfix_aitype',
            'NOW_Y_BLOCK_PTSub_CTY1': 'now_y_block_ptsub_cty1',
            'NOW_Y_BLOCK_PTSub_CTY2': 'now_y_block_ptsub_cty2',
            'NOW_Y_BLOCK_PTSub_OSType': 'now_y_block_ptsub_ostype',
            'NOW_Y_BLOCK_Funnel_FU1': 'now_y_block_funnel_fu1',
            'NOW_Y_BLOCK_Funnel_FU2': 'now_y_block_funnel_fu2',
            'NOW_Y_BLOCK_Channel_CH': 'now_y_block_channel_ch',
            'NOW_Y_BLOCK_Employee_EGT1': 'now_y_block_employee_egt1',
            'NOW_Y_BLOCK_Employee_EGT2': 'now_y_block_employee_egt2',
            'NOW_Y_BLOCK_Employee_EGT3': 'now_y_block_employee_egt3',
            'NOW_Y_BLOCK_Employee_EGT4': 'now_y_block_employee_egt4',
            'NOW_Y_BLOCK_HR_HR1': 'now_y_block_hr_hr1',
            'NOW_Y_BLOCK_HR_HR2': 'now_y_block_hr_hr2',
            'NOW_Y_BLOCK_HR_HR3': 'now_y_block_hr_hr3',
            'NOW_Y_BLOCK_SEC': 'now_y_block_sec',
            'NOW_Y_BLOCK_Period_MX': 'now_y_block_period_mx',
            'NOW_Y_BLOCK_Period_DX': 'now_y_block_period_dx',
            'NOW_Y_BLOCK_Period_PPC': 'now_y_block_period_ppc',
            'NOW_Y_BLOCK_Period_NP': 'now_y_block_period_np',
            'NOW_Y_BLOCK_LE_LE1': 'now_y_block_le_le1',
            'NOW_Y_BLOCK_LE_LE2': 'now_y_block_le_le2',
            'NOW_Y_BLOCK_UNIT': 'now_y_block_unit',
            'NOW_Y_BLOCK_TD_BU': 'now_y_block_td_bu'
        }

        rep_cells_to_insert = []

        # If filter_combinations is empty, create a list with one empty dict to run the logic once without filters
        filter_items_to_process = filter_combinations if filter_combinations else [{}]
        print(
            f"[INFO] Processing {len(filter_items_to_process)} filter combinations (empty={len(filter_combinations) == 0})")

        for my_filter_item in filter_items_to_process:
            # Step120 Foreach MyFilterItem
            l_items = list(range(120))

            # Step140 Calculate all MyXPeriod values for all L values
            x_period_list = [calculate_x_period(my_last_report_month, l) for l in l_items]
            print(f"[INFO][Step 140] Calculated {len(x_period_list)} periods: {x_period_list}")

            # Step160-180 Query all SOCell data at once
            plan_data, actual_data, forecast_data = query_so_cell_data(
                bq, project_id, my_rep_page, my_kr_type_full, my_filter_item, x_period_list, my_rep_page.now_zblock2_alt
            )

            # Collect RepCell records for batch insert

            # Now loop through each L and process the data
            for l in l_items:
                my_x_period = x_period_list[l]
                my_y_number_3 = l

                so_cell1_now_value = plan_data.get(my_x_period)
                so_cell2_now_value = actual_data.get(my_x_period)
                so_cell3_now_value = forecast_data.get(my_x_period)

                print(
                    f"[INFO][Step 140-180] L={l}, MyXPeriod={my_x_period}, Plan={so_cell1_now_value}, Actual={so_cell2_now_value}, Forecast={so_cell3_now_value}")

                # Step190 Create RepCell for Plan
                rep_cell_plan = RepCell(
                    z_number=my_z_number,
                    y_number1=my_y_number_1,
                    y_number2=my_y_number_2,
                    y_number3=my_y_number_3,
                    my_rep_page=rep_page_identifier,
                    my_rep_temp_block=my_rep_temp,
                    z_block_type="Plan",
                    now_np=my_x_period,
                    now_value=so_cell1_now_value
                )

                for kr_field, kr_value in my_kr_type_full.items():
                    if kr_field in kr_field_map:
                        setattr(rep_cell_plan, kr_field_map[kr_field], kr_value)

                for filter_field, filter_value in my_filter_item.items():
                    if filter_field in filter_field_map:
                        setattr(rep_cell_plan, filter_field_map[filter_field], filter_value)

                rep_cells_to_insert.append(rep_cell_plan.to_bigquery_dict())
                print(f"[INFO][Step 190] Prepared RepCell (Plan): z_number={my_z_number}, "
                      f"y_number1={my_y_number_1}, y_number2={my_y_number_2}, y_number3={my_y_number_3}, "
                      f"now_value={so_cell1_now_value}")

                # Step200 Create RepCell for ActualForecast
                last_actual_month_year = int(my_last_actual_month[1:3])
                last_actual_month_month = int(my_last_actual_month[3:5])
                my_x_period_year = int(my_x_period[1:3])
                my_x_period_month = int(my_x_period[3:5])

                last_actual_total = last_actual_month_year * 12 + last_actual_month_month
                my_x_period_total = my_x_period_year * 12 + my_x_period_month

                if last_actual_total >= my_x_period_total:
                    rep_cell_actual_forecast = RepCell(
                        z_number=my_z_number,
                        y_number1=my_y_number_1,
                        y_number2=my_y_number_2,
                        y_number3=my_y_number_3,
                        my_rep_page=rep_page_identifier,
                        my_rep_temp_block=my_rep_temp,
                        z_block_type="ActualForecast",
                        now_np=my_x_period,
                        now_value=so_cell2_now_value
                    )
                    print(f"[INFO][Step 200] Prepared RepCell (ActualForecast-Actual): "
                          f"LastActualMonth={my_last_actual_month} >= MyXPeriod={my_x_period}, "
                          f"now_value={so_cell2_now_value}")
                else:
                    rep_cell_actual_forecast = RepCell(
                        z_number=my_z_number,
                        y_number1=my_y_number_1,
                        y_number2=my_y_number_2,
                        y_number3=my_y_number_3,
                        my_rep_page=rep_page_identifier,
                        my_rep_temp_block=my_rep_temp,
                        z_block_type="ActualForecast",
                        now_np=my_x_period,
                        now_value=so_cell3_now_value
                    )
                    print(f"[INFO][Step 200] Prepared RepCell (ActualForecast-Forecast): "
                          f"LastActualMonth={my_last_actual_month} < MyXPeriod={my_x_period}, "
                          f"now_value={so_cell3_now_value}")

                for kr_field, kr_value in my_kr_type_full.items():
                    if kr_field in kr_field_map:
                        setattr(rep_cell_actual_forecast, kr_field_map[kr_field], kr_value)

                for filter_field, filter_value in my_filter_item.items():
                    if filter_field in filter_field_map:
                        setattr(rep_cell_actual_forecast, filter_field_map[filter_field], filter_value)

                rep_cells_to_insert.append(rep_cell_actual_forecast.to_bigquery_dict())

            # Batch insert all RepCell records for this filter_item
        if rep_cells_to_insert:
            bq.insert_rows(
                dataset_id=settings.REPORT_DATASET_NAME,
                table_id=settings.REP_CELL_TABLE_NAME,
                rows_data=rep_cells_to_insert
            )
            print(f"[INFO][Step 190-200] Batch inserted {len(rep_cells_to_insert)} RepCell records for filter_item")

    # Get RepPage identifier (using z_block_plan as identifier)
    print(f"[INFO] build_report completed successfully. RepPage: {rep_page_identifier}")
    return rep_page_identifier