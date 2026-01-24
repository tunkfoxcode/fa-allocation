import pandas as pd
from config.field_mappings import YBLOCK_FIELD_MAPPING, PREV_YBLOCK_FIELD_MAPPING
from typing import List, Dict
from collections import defaultdict


def build_so_cell_query(allocation_by_type_item, project_id: str, my_x_period: str = None, 
                        dataset_id: str = 'alloc_stage', table_id: str = 'so_cell_raw_full') -> str:
    """
    Build dynamic query cho SoCell dựa trên Y-block fields của AllocationByType

    Args:
        allocation_by_type_item: Instance của AllocationByType
        project_id: Google Cloud Project ID
        my_x_period: Period value to filter by now_np (optional)
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

    # Add now_np condition if my_x_period is provided
    if my_x_period is not None and not pd.isna(my_x_period):
        if isinstance(my_x_period, str):
            escaped_period = my_x_period.replace("'", "\\'")
            where_conditions.append(f"now_np = '{escaped_period}'")
        else:
            where_conditions.append(f"now_np = {my_x_period}")

    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"

    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)

    return query


def build_so_cell_batch_query(allocation_by_type_items, project_id: str, my_x_period: str = None,
                               dataset_id: str = 'alloc_stage', table_id: str = 'so_cell_raw_full') -> str:
    """
    Build batch query cho nhiều AllocationByType items sử dụng OR conditions.
    Query một lần thay vì query nhiều lần trong loop.
    
    Args:
        allocation_by_type_items: List of AllocationByType instances
        project_id: Google Cloud Project ID
        my_x_period: Period value to filter by now_np (optional)
        dataset_id: Dataset ID (default: 'alloc_stage')
        table_id: Table ID (default: 'so_cell_raw_full')
        
    Returns:
        SQL query string với WHERE conditions sử dụng OR cho từng item
    """
    if not allocation_by_type_items:
        return None
    
    or_conditions = []
    
    for allocation_by_type_item in allocation_by_type_items:
        # Skip GAgg and ByAgg types
        if hasattr(allocation_by_type_item, 'by_block_by_type'):
            if allocation_by_type_item.by_block_by_type in ['GAgg', 'ByAgg']:
                continue
        
        and_conditions = []
        
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
            
            and_conditions.append(f"{so_cell_field} = {formatted_value}")
        
        if and_conditions:
            or_conditions.append(f"({' AND '.join(and_conditions)})")
    
    if not or_conditions:
        return None
    
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`\nWHERE ("
    query += "\nOR ".join(or_conditions)
    query += ")"
    
    # Add now_np condition if my_x_period is provided
    if my_x_period is not None and not pd.isna(my_x_period):
        if isinstance(my_x_period, str):
            escaped_period = my_x_period.replace("'", "\\'")
            query += f"\nAND now_np = '{escaped_period}'"
        else:
            query += f"\nAND now_np = {my_x_period}"
    
    return query


def create_allocation_key(allocation_by_type_item) -> str:
    """
    Tạo unique key từ AllocationByType item dựa trên các Y-block fields.
    Key này dùng để group và lookup SoCell results.
    
    Args:
        allocation_by_type_item: Instance của AllocationByType
        
    Returns:
        String key dạng "field1:value1|field2:value2|..."
    """
    key_parts = []
    
    for by_type_field, so_cell_field in YBLOCK_FIELD_MAPPING.items():
        value = getattr(allocation_by_type_item, by_type_field, None)
        
        if value is None or pd.isna(value) or (isinstance(value, str) and value == ''):
            continue
        
        key_parts.append(f"{so_cell_field}:{value}")
    
    return "|".join(sorted(key_parts))


def create_socell_key(so_cell_item) -> str:
    """
    Tạo unique key từ SoCell item dựa trên các now_y_block fields.
    Key này phải match với key từ create_allocation_key.
    
    Args:
        so_cell_item: Instance của SoCell
        
    Returns:
        String key dạng "field1:value1|field2:value2|..."
    """
    key_parts = []
    
    for by_type_field, so_cell_field in YBLOCK_FIELD_MAPPING.items():
        value = getattr(so_cell_item, so_cell_field, None)
        
        if value is None or pd.isna(value) or (isinstance(value, str) and value == ''):
            continue
        
        key_parts.append(f"{so_cell_field}:{value}")
    
    return "|".join(sorted(key_parts))


def group_socell_by_allocation(so_cell_items: List) -> Dict[str, List]:
    """
    Group SoCell items theo key để lookup nhanh.
    
    Args:
        so_cell_items: List of SoCell instances
        
    Returns:
        Dictionary mapping key -> list of SoCell items
    """
    grouped = defaultdict(list)
    
    for so_cell_item in so_cell_items:
        key = create_socell_key(so_cell_item)
        grouped[key].append(so_cell_item)
    
    return dict(grouped)


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
