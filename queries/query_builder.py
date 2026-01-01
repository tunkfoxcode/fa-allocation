import pandas as pd
from config.field_mappings import YBLOCK_FIELD_MAPPING, PREV_YBLOCK_FIELD_MAPPING


def build_so_cell_query(allocation_by_type_item, project_id: str, dataset_id: str = 'alloc_stage',
                        table_id: str = 'so_cell_raw_full') -> str:
    """
    Build dynamic query cho SoCell dựa trên Y-block fields của AllocationByType

    Args:
        allocation_by_type_item: Instance của AllocationByType
        project_id: Google Cloud Project ID
        dataset_id: Dataset ID (default: 'alloc_stage')
        table_id: Table ID (default: 'so_cell_raw_full')

    Returns:
        SQL query string với WHERE conditions động
    """
    where_conditions = []

    for by_type_field, so_cell_field in YBLOCK_FIELD_MAPPING.items():
        value = getattr(allocation_by_type_item, by_type_field, None)

        if value is None:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, str) and value == '':
            continue

        if isinstance(value, str):
            escaped_value = value.replace("'", "\\'")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"

        where_conditions.append(f"{so_cell_field} = {formatted_value}")

    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"

    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)

    return query


def build_so_cell_by_kr_query(allocation_by_kr_item,
                              allocation_to_item,
                              project_id: str,
                              to_item: str,
                              dataset_id: str = 'alloc_stage',
                              table_id: str = 'so_cell_raw_full',
                              ) -> str:
    """
    Build dynamic query cho SoCell dựa trên Y-block fields từ AllocationByKR và AllocationToItem

    Args:
        allocation_by_kr_item: Instance của AllocationByKR (kr_block_3)
        allocation_to_item: Instance của AllocationToItem (filter_block_3)
        project_id: Google Cloud Project ID
        to_item: ToItem value
        dataset_id: Dataset ID (default: 'alloc_stage')
        table_id: Table ID (default: 'so_cell_raw_full')

    Returns:
        SQL query string với WHERE conditions động
    """
    where_conditions = []

    kr_to_socell_mapping = {
        'to_y_block_kr1': 'now_y_block_kr_item_code_kr1',
        'to_y_block_kr2': 'now_y_block_kr_item_code_kr2',
        'to_y_block_kr3': 'now_y_block_kr_item_code_kr3',
        'to_y_block_kr4': 'now_y_block_kr_item_code_kr4',
        'to_y_block_kr5': 'now_y_block_kr_item_code_kr5',
        'to_y_block_kr6': 'now_y_block_kr_item_code_kr6',
        'to_y_block_kr7': 'now_y_block_kr_item_code_kr7',
        'to_y_block_kr8': 'now_y_block_kr_item_code_kr8',
        'to_y_block_cdt1': 'now_y_block_cdt_cdt1',
        'to_y_block_cdt2': 'now_y_block_cdt_cdt2',
        'to_y_block_cdt3': 'now_y_block_cdt_cdt3',
        'to_y_block_cdt4': 'now_y_block_cdt_cdt4',
        'to_y_block_pt1': 'now_y_block_ptnow_pt1',
        'to_y_block_pt2': 'now_y_block_ptnow_pt2',
        'to_y_block_duration': 'now_y_block_ptnow_duration',
        'to_y_block_pt1_prev': 'now_y_block_ptprev_pt1',
        'to_y_block_pt2_prev': 'now_y_block_ptprev_pt2',
        'to_y_block_duration_prev': 'now_y_block_ptprev_duration',
        'to_y_block_own_type': 'now_y_block_ptfix_owntype',
        'to_y_block_ai_type': 'now_y_block_ptfix_aitype',
        'to_y_block_cty1': 'now_y_block_ptsub_cty1',
        'to_y_block_cty2': 'now_y_block_ptsub_cty2',
        'to_y_block_os_type': 'now_y_block_ptsub_ostype',
        'to_y_block_fu1': 'now_y_block_funnel_fu1',
        'to_y_block_fu2': 'now_y_block_funnel_fu2',
        'to_y_block_ch': 'now_y_block_channel_ch',
        'to_y_block_egt1': 'now_y_block_employee_egt1',
        'to_y_block_egt2': 'now_y_block_employee_egt2',
        'to_y_block_egt3': 'now_y_block_employee_egt3',
        'to_y_block_egt4': 'now_y_block_employee_egt4',
        'to_y_block_hr1': 'now_y_block_hr_hr1',
        'to_y_block_hr2': 'now_y_block_hr_hr2',
        'to_y_block_hr3': 'now_y_block_hr_hr3',
        'to_y_block_sec': 'now_y_block_sec',
        'to_y_block_mx': 'now_y_block_period_mx',
        'to_y_block_dx': 'now_y_block_period_dx',
        'to_y_block_ppc': 'now_y_block_period_ppc',
        'to_y_block_np': 'now_y_block_period_np',
        'to_y_block_le1': 'now_y_block_le_le1',
        'to_y_block_le2': 'now_y_block_le_le2',
        'to_y_block_unit': 'now_y_block_unit',
    }

    for kr_field, socell_field in kr_to_socell_mapping.items():
        value = getattr(allocation_by_kr_item, kr_field, None)

        if value is None:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, str) and value == '':
            continue

        if isinstance(value, str):
            escaped_value = value.replace("'", "\\'")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"

        where_conditions.append(f"{socell_field} = {formatted_value}")

    where_conditions.append(f"now_y_block_period_mx = '{to_item}'")

    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"

    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)

    return query


def build_so_cell_prev_query(y_block_1, x_period_1: str, z_number: int, project_id: str,
                             dataset_id: str = 'alloc_stage', table_id: str = 'so_cell_raw_full') -> str:
    """
    Build dynamic query cho SoCell dựa trên PrevYBlock matching với NowYBlock của y_block_1

    Args:
        y_block_1: SoCell instance có NowYBlock cần match với PrevYBlock
        x_period_1: XPeriod value (now_np)
        z_number: ZNumber từ AllocationALT
        project_id: Google Cloud Project ID
        dataset_id: Dataset ID (default: 'alloc_stage')
        table_id: Table ID (default: 'so_cell_raw_full')

    Returns:
        SQL query string với WHERE conditions động
    """
    where_conditions = []

    for now_field, prev_field in PREV_YBLOCK_FIELD_MAPPING.items():
        value = getattr(y_block_1, now_field, None)

        if value is None:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, str) and value == '':
            continue

        if isinstance(value, str):
            escaped_value = value.replace("'", "\\'")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"

        where_conditions.append(f"{prev_field} = {formatted_value}")

    if x_period_1 is not None and not pd.isna(x_period_1):
        if isinstance(x_period_1, str):
            escaped_x_period = x_period_1.replace("'", "\\'")
            where_conditions.append(f"now_np = '{escaped_x_period}'")
        else:
            where_conditions.append(f"now_np = {x_period_1}")

    if z_number is not None:
        where_conditions.append(f"now_zblock2_alt = '{z_number}'")

    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"

    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)

    return query
