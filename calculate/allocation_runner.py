import copy
from db.bigquery_connector import BigQueryConnector
from models.allocation_models import AllocationALT, AllocationToItem, AllocationByType, AllocationByKR
from models.so_cell_model import SoCell
from queries.query_builder import (
    build_so_cell_query, 
    build_so_cell_by_kr_query, 
    build_so_cell_prev_query,
    build_so_cell_batch_query,
    build_so_cell_prev_batch_query,
    build_so_cell_by_kr_batch_query,
    create_allocation_key,
    group_socell_by_allocation,
    create_prev_socell_key,
    group_prev_socell_by_key,
    group_by_percent_results
)
from utils.period_utils import add_period_strings
from services.allocation_service import calculate_offset
from services.so_cell_factory import create_socell_from_yblocks


def query_allocation_items(
        bq: BigQueryConnector,
        project_id: str,
        allocation_config_dataset_name: str,
        allocation_to_item_table_name: str,
        allocation_by_type_table_name: str,
        my_allocation_alt_item: AllocationALT
):
    """
    Query AllocationToItem and AllocationByType for a given AllocationALT item.
    
    Args:
        bq: BigQueryConnector instance
        project_id: GCP project ID
        allocation_config_dataset_name: Dataset name for allocation config
        allocation_to_item_table_name: Table name for AllocationToItem
        allocation_by_type_table_name: Table name for AllocationByType
        my_allocation_alt_item: AllocationALT item to query for
        
    Returns:
        Tuple of (my_to_items, my_allocation_by_type_items)
    """
    # Query AllocationToItem
    query_to_item = f"""
    SELECT * 
    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
    WHERE TO_Y_BLOCK_ToType = "{my_allocation_alt_item.to_type}"
    """
    my_to_items_raw = bq.execute_query(query_to_item)
    my_to_items = AllocationToItem.from_dataframe(my_to_items_raw)
    
    print(f"[INFO][Step 40] We having {len(my_to_items)} my_to_items by query: \n {query_to_item}")
    
    # Query AllocationByType
    query_by_type = f"""
    SELECT * 
    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_type_table_name}` 
    WHERE ZNumber = {my_allocation_alt_item.z_number}
    ORDER BY YNumber DESC
    """
    
    my_by_type_raw = bq.execute_query(query_by_type)
    my_allocation_by_type_items = AllocationByType.from_dataframe(my_by_type_raw)
    
    print(f"[INFO][Step 50] We having {len(my_allocation_by_type_items)} my_allocation_by_type_items by query: \n {query_by_type}")
    
    return my_to_items, my_allocation_by_type_items


def process_by_agg_allocation(
        bq: BigQueryConnector,
        project_id: str,
        allocation_config_dataset_name: str,
        allocation_to_item_table_name: str,
        alloc_data_dataset_name: str,
        so_cell_table_name: str
):
    """
    Process ByAgg allocation type by querying PT-S2, CTY-S2, and NP-D365 items,
    then aggregating and inserting SoCell data.
    
    Args:
        bq: BigQueryConnector instance
        project_id: GCP project ID
        allocation_config_dataset_name: Dataset name for allocation config
        allocation_to_item_table_name: Table name for AllocationToItem
        alloc_data_dataset_name: Dataset name for allocation data
        so_cell_table_name: Table name for SoCell
    """
    # Query PT-S2 items
    query_pt_s2 = f"""
    SELECT TO_Y_BLOCK_ToItem 
    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
    WHERE TO_Y_BLOCK_ToType = 'PT-S2'
    """
    pt_s2_raw = bq.execute_query(query_pt_s2)
    pt_s2_items = [str(item) for item in pt_s2_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()]
    
    # Query CTY-S2 items
    query_cty_s2 = f"""
    SELECT TO_Y_BLOCK_ToItem 
    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
    WHERE TO_Y_BLOCK_ToType = 'CTY-S2'
    """
    cty_s2_raw = bq.execute_query(query_cty_s2)
    cty_s2_items = [str(item) for item in cty_s2_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()]
    
    # Query NP-D365 items
    query_np_d365 = f"""
    SELECT TO_Y_BLOCK_ToItem 
    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
    WHERE TO_Y_BLOCK_ToType = 'NP-D365'
    """
    np_d365_raw = bq.execute_query(query_np_d365)
    np_d365_items = [str(item) for item in np_d365_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()]
    
    # Process all combinations
    for my_to_item_pts2 in pt_s2_items:
        for my_to_item_ctys2 in cty_s2_items:
            for my_to_item_np_d365 in np_d365_items:
                # Query and aggregate SoCell data
                query_so_cell_byagg = f"""
                SELECT * 
                FROM `{project_id}.{alloc_data_dataset_name}.{so_cell_table_name}` 
                WHERE now_y_block_fnf_fnf = "KRN"
                AND now_y_block_kr_item_code_kr1 = "NO"
                AND now_y_block_kr_item_code_kr2 = "GI"
                AND now_y_block_kr_item_code_kr3 = "DAU"
                AND now_y_block_kr_item_code_kr4 = "DC"
                AND now_y_block_kr_item_code_kr5 = "NP"
                AND CONCAT(now_y_block_ptnow_pt1, now_y_block_ptnow_pt2) = '{my_to_item_pts2}'
                AND CONCAT(now_y_block_ptsub_cty1, now_y_block_ptsub_cty2) = '{my_to_item_ctys2}'
                AND NOW_NP = '{my_to_item_np_d365}'
                AND now_y_block_cdt_cdt1 IS NOT NULL
                AND now_y_block_cdt_cdt2 IS NOT NULL
                AND now_y_block_cdt_cdt3 IS NOT NULL
                AND now_y_block_cdt_cdt4 IS NOT NULL
                """
                so_cell_byagg_raw = bq.execute_query(query_so_cell_byagg)
                so_cell_byagg_items = SoCell.from_dataframe(so_cell_byagg_raw)
                
                # Aggregate values
                value_2 = 0
                for so_cell_byagg_item in so_cell_byagg_items:
                    value_1 = so_cell_byagg_item.now_value
                    if value_1 is None:
                        continue
                    value_2 = value_2 + value_1
                
                # Split PT and CTY codes
                pt1 = my_to_item_pts2[:2] if len(my_to_item_pts2) >= 2 else my_to_item_pts2
                pt2 = my_to_item_pts2[2:] if len(my_to_item_pts2) > 2 else None
                cty1 = my_to_item_ctys2[:2] if len(my_to_item_ctys2) >= 2 else my_to_item_ctys2
                cty2 = my_to_item_ctys2[2:] if len(my_to_item_ctys2) > 2 else None
                
                # Create aggregated SoCell record
                insert_so_cell_byagg = SoCell(
                    now_y_block_fnf_fnf="KRN",
                    now_y_block_kr_item_code_kr1="NO",
                    now_y_block_kr_item_code_kr2="GI",
                    now_y_block_kr_item_code_kr3="DAU",
                    now_y_block_kr_item_code_kr4="DC",
                    now_y_block_kr_item_code_kr5="NP",
                    now_y_block_kr_item_code_kr6=None,
                    now_y_block_kr_item_code_kr7=None,
                    now_y_block_kr_item_code_kr8=None,
                    now_y_block_cdt_cdt1=None,
                    now_y_block_cdt_cdt2=None,
                    now_y_block_cdt_cdt3=None,
                    now_y_block_cdt_cdt4=None,
                    now_y_block_ptnow_pt1=pt1,
                    now_y_block_ptnow_pt2=pt2,
                    now_y_block_ptnow_duration=None,
                    now_y_block_ptprev_pt1=None,
                    now_y_block_ptprev_pt2=None,
                    now_y_block_ptprev_duration=None,
                    now_y_block_ptfix_owntype=None,
                    now_y_block_ptfix_aitype=None,
                    now_y_block_ptsub_cty1=cty1,
                    now_y_block_ptsub_cty2=cty2,
                    now_y_block_ptsub_ostype=None,
                    now_y_block_funnel_fu1=None,
                    now_y_block_funnel_fu2=None,
                    now_y_block_channel_ch=None,
                    now_y_block_employee_egt1=None,
                    now_y_block_employee_egt2=None,
                    now_y_block_employee_egt3=None,
                    now_y_block_employee_egt4=None,
                    now_y_block_hr_hr1=None,
                    now_y_block_hr_hr2=None,
                    now_y_block_hr_hr3=None,
                    now_y_block_sec=None,
                    now_y_block_period_mx=None,
                    now_y_block_period_dx=None,
                    now_y_block_period_ppc=None,
                    now_y_block_period_np=None,
                    now_y_block_le_le1=None,
                    now_y_block_le_le2=None,
                    now_y_block_unit=None,
                    now_np=my_to_item_np_d365,
                    now_value=value_2,
                    prev_y_block_kr_item_code_kr1=None,
                    prev_y_block_kr_item_code_kr2=None,
                    prev_y_block_kr_item_code_kr3=None,
                    prev_y_block_kr_item_code_kr4=None,
                    prev_y_block_kr_item_code_kr5=None,
                    prev_y_block_kr_item_code_kr6=None,
                    prev_y_block_kr_item_code_kr7=None,
                    prev_y_block_kr_item_code_kr8=None,
                    prev_y_block_cdt_cdt1=None,
                    prev_y_block_cdt_cdt2=None,
                    prev_y_block_cdt_cdt3=None,
                    prev_y_block_cdt_cdt4=None,
                    prev_y_block_ptnow_pt1=None,
                    prev_y_block_ptnow_pt2=None,
                    prev_y_block_ptnow_duration=None,
                    prev_y_block_ptprev_pt1=None,
                    prev_y_block_ptprev_pt2=None,
                    prev_y_block_ptprev_duration=None,
                    prev_y_block_ptfix_owntype=None,
                    prev_y_block_ptfix_aitype=None,
                    prev_y_block_ptsub_cty1=None,
                    prev_y_block_ptsub_cty2=None,
                    prev_y_block_ptsub_ostype=None,
                    prev_y_block_funnel_fu1=None,
                    prev_y_block_funnel_fu2=None,
                    prev_y_block_channel_ch=None,
                    prev_y_block_employee_egt1=None,
                    prev_y_block_employee_egt2=None,
                    prev_y_block_employee_egt3=None,
                    prev_y_block_employee_egt4=None,
                    prev_y_block_hr_hr1=None,
                    prev_y_block_hr_hr2=None,
                    prev_y_block_hr_hr3=None,
                    prev_y_block_sec=None,
                    prev_y_block_period_mx=None,
                    prev_y_block_period_dx=None,
                    prev_y_block_period_np=None,
                    prev_y_block_le_le1=None,
                    prev_y_block_le_le2=None,
                    prev_y_block_unit=None,
                    prev_ppc=None,
                    prev_value=None,
                    by_block_bytype='ByAgg',
                    by_block_bypercent=None
                )
                
                # Insert aggregated record
                success = bq.insert_row(
                    dataset_id=alloc_data_dataset_name,
                    table_id=so_cell_table_name,
                    row_data=insert_so_cell_byagg
                )
                if success:
                    print(f"[INFO] ByAgg: Successfully inserted aggregated SoCell with value={value_2}, PTS2={my_to_item_pts2}, CTYS2={my_to_item_ctys2}, NPD365={my_to_item_np_d365}")
                else:
                    print(f"[ERROR] ByAgg: Failed to insert aggregated SoCell")


def run_allocate(
        min_alt,
        max_alt,
        my_x_period
):
    """
    Main allocation calculation workflow.
    Orchestrates the entire allocation process from reading configuration
    to calculating and inserting results into BigQuery.
    """
    credentials_path = "/home/tunk/Desktop/fp-a-project-0c82aa55ae6a.json"
    project_id = "fp-a-project"

    allocation_config_dataset_name = "allocation_config"
    alloc_data_dataset_name = "alloc_stage"

    allocation_alt_table_name = "AllocationALT_NativeTable"
    allocation_to_item_table_name = "AllocationToItem_NativeTable"
    allocation_by_type_table_name = "AllocationByType_NativeTable"
    allocation_by_kr_table_name = "AllocationByKR_NativeTable"
    so_cell_table_name = "so_cell_raw_full"

    try:
        bq = BigQueryConnector(
            credentials_path=credentials_path,
            project_id=project_id
        )

        query = f"""
        SELECT
            ZNumber,
            FROM_ALT_FromALT,
            TO_ALT_ToALT,
            FROM_Y_BLOCK_FromType,
            TO_Y_BLOCK_ToType
        FROM `{project_id}.{allocation_config_dataset_name}.{allocation_alt_table_name}` 
        WHERE ZNumber >= min_alt AND ZNumber <= max_alt
        ORDER BY ZNumber 
        """
        df = bq.execute_query(query)
        my_allocation_alt_items = AllocationALT.from_dataframe(df)

        print(f"[INFO][Step 20] We having {len(my_allocation_alt_items)} my_allocation_alt_items by query: \n {query}")

        for my_allocation_alt_item in my_allocation_alt_items:
            if my_allocation_alt_item.z_number != 422:
                print(
                    f"[WARN][Step 30] Skip process my_allocation_alt_item: {my_allocation_alt_item} because z_number is not equal 422 for testing purpose, remove it when calculating in production mode")
                continue
            print(f"[INFO][Step 30] Start process each my_allocation_alt_item: {my_allocation_alt_item}")

            # Initialize list to collect all insert records for batch insert
            batch_insert_records = []

            # Query AllocationToItem and AllocationByType
            #Step35-Step50
            my_to_items, my_allocation_by_type_items = query_allocation_items(
                bq=bq,
                project_id=project_id,
                allocation_config_dataset_name=allocation_config_dataset_name,
                allocation_to_item_table_name=allocation_to_item_table_name,
                allocation_by_type_table_name=allocation_by_type_table_name,
                my_allocation_alt_item=my_allocation_alt_item
            )

            # Step55: Batch query all allocation_by_kr items for this alt_item
            print(f"[INFO][Step 55] Building batch query for allocation_by_kr items")
            my_from_type = my_allocation_alt_item.from_type
            my_to_type = my_allocation_alt_item.to_type
            
            # Get unique by_types from allocation_by_type_items
            by_types = set()
            for item in my_allocation_by_type_items:
                if item.by_block_by_type and item.by_block_by_type not in ['GAgg', 'ByAgg']:
                    # Only include numeric by_types
                    if str(item.by_block_by_type).lstrip('-').isdigit():
                        continue
                    by_types.add(item.by_block_by_type)
            
            if by_types:
                by_types_list = list(by_types)
                by_types_in_clause = "', '".join(by_types_list)
                
                query_allocation_by_kr_batch = f"""
                SELECT * 
                FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_kr_table_name}` 
                WHERE TO_Y_BLOCK_KR6 = '{my_from_type}'
                AND TO_Y_BLOCK_KR4 = '{my_to_type}'
                AND BY_BLOCK_ByType IN ('{by_types_in_clause}')
                """
                
                print(f"[INFO][Step 55] Executing batch allocation_by_kr query for {len(by_types)} by_types")
                allocation_by_kr_raw = bq.execute_query(query_allocation_by_kr_batch)
                all_allocation_by_kr_items = AllocationByKR.from_dataframe(allocation_by_kr_raw)
                print(f"[INFO][Step 55] Batch query returned {len(all_allocation_by_kr_items)} allocation_by_kr items")
                
                # Create map: (from_type, to_type, by_type) -> allocation_by_kr_item
                allocation_by_kr_map = {}
                for kr_item in all_allocation_by_kr_items:
                    key = (my_from_type, my_to_type, kr_item.by_block_by_type)
                    if key not in allocation_by_kr_map:
                        allocation_by_kr_map[key] = kr_item
                
                print(f"[INFO][Step 55] Created allocation_by_kr_map with {len(allocation_by_kr_map)} keys")
            else:
                allocation_by_kr_map = {}
                print(f"[INFO][Step 55] No valid by_types found, allocation_by_kr_map is empty")

            # Step60: Process ByAgg types first
            for my_allocation_by_type_item in my_allocation_by_type_items:
                if my_allocation_by_type_item.by_block_by_type == 'ByAgg':
                    print(f"[INFO][Step 60] Processing ByAgg allocation")
                    process_by_agg_allocation(
                        bq=bq,
                        project_id=project_id,
                        allocation_config_dataset_name=allocation_config_dataset_name,
                        allocation_to_item_table_name=allocation_to_item_table_name,
                        alloc_data_dataset_name=alloc_data_dataset_name,
                        so_cell_table_name=so_cell_table_name
                    )

            # Step70: Batch query all SoCell data once (excluding GAgg and ByAgg)
            print(f"[INFO][Step 70] Building batch query for all allocation_by_type_items")
            query_so_cell_batch = build_so_cell_batch_query(
                my_allocation_by_type_items,
                project_id,
                my_x_period=my_x_period,
                dataset_id=alloc_data_dataset_name,
                table_id=so_cell_table_name
            )
            
            if query_so_cell_batch is None:
                print(f"[WARN][Step 70] No valid allocation_by_type_items to query, skipping")
                continue
            
            print(f"[INFO][Step 70] Executing batch query: \n{query_so_cell_batch}")
            my_so_cell_raw = bq.execute_query(query_so_cell_batch)
            all_so_cell_items = SoCell.from_dataframe(my_so_cell_raw)
            print(f"[INFO][Step 70] Batch query returned {len(all_so_cell_items)} SoCell items")
            
            # Group SoCell items by key for fast lookup
            so_cell_map = group_socell_by_allocation(all_so_cell_items)
            print(f"[INFO][Step 70] Grouped into {len(so_cell_map)} unique keys")

            # Step80: Process each allocation_by_type_item using the map
            for my_allocation_by_type_item in my_allocation_by_type_items:
                print(f"[INFO][Step 80] Processing my_allocation_by_type_item: {my_allocation_by_type_item}")

                if my_allocation_by_type_item.by_block_by_type == 'GAgg':
                    print("[INFO][Step 80] Skipping GAgg type")
                    continue

                if my_allocation_by_type_item.by_block_by_type == 'ByAgg':
                    print("[INFO][Step 80] Skipping ByAgg type (already processed)")
                    continue
                
                # Lookup SoCell items from map using key
                allocation_key = create_allocation_key(my_allocation_by_type_item)
                from_so_cell_items = so_cell_map.get(allocation_key, [])
                print(f"[INFO][Step 80] Found {len(from_so_cell_items)} SoCell items for key: {allocation_key[:100]}...")

                if not from_so_cell_items:
                    print(f"[INFO][Step 80] No SoCell items found, skipping")
                    continue

                # Step90: Batch query all prev SoCells for this allocation_by_type_item
                print(f"[INFO][Step 90] Building batch query for {len(from_so_cell_items)} prev SoCells")
                query_so_cell_prev_batch = build_so_cell_prev_batch_query(
                    from_so_cell_items=from_so_cell_items,
                    z_number=my_allocation_alt_item.z_number,
                    project_id=project_id,
                    dataset_id=alloc_data_dataset_name,
                    table_id=so_cell_table_name
                )
                
                if query_so_cell_prev_batch is None:
                    print(f"[WARN][Step 90] No valid prev query conditions, skipping")
                    continue
                
                print(f"[INFO][Step 90] Executing batch prev query")
                so_cell_prev_raw = bq.execute_query(query_so_cell_prev_batch)
                all_prev_so_cell_items = SoCell.from_dataframe(so_cell_prev_raw)
                print(f"[INFO][Step 90] Batch prev query returned {len(all_prev_so_cell_items)} prev SoCell items")
                
                # Group prev SoCell items by key for fast lookup
                prev_so_cell_map = group_prev_socell_by_key(all_prev_so_cell_items)
                print(f"[INFO][Step 90] Grouped prev SoCells into {len(prev_so_cell_map)} unique keys")

                # Step100: Process each from_so_cell_item using the prev map
                for from_so_cell_item in from_so_cell_items:
                    print(f"[INFO][Step 100] Start processing for each from_so_cell_item: {from_so_cell_item}")
                    y_block_1 = from_so_cell_item
                    x_period_1 = from_so_cell_item.now_np
                    value_1 = from_so_cell_item.now_value
                    print(
                        f"[INFO][Step 100] We have y_block_1: {y_block_1}, x_period_1: {x_period_1}, value_1: {value_1}")

                    # Lookup prev SoCell items from map using key
                    prev_key = create_prev_socell_key(y_block_1)
                    so_cells_prev_y_block = prev_so_cell_map.get(prev_key, [])
                    print(f"[INFO][Step 110] Found {len(so_cells_prev_y_block)} prev SoCell items for key: {prev_key[:100]}...")

                    if len(so_cells_prev_y_block) > 0:
                        print("[WARN][Step 120] Skip process because so_cells_prev_y_block is empty (N=0)")
                        continue

                    if my_allocation_by_type_item.by_block_by_type and str(
                            my_allocation_by_type_item.by_block_by_type).lstrip('-').isdigit():
                        calculate_offset(
                            bq=bq,
                            my_allocation_by_type_item=my_allocation_by_type_item,
                            my_allocation_alt_item=my_allocation_alt_item,
                            y_block_1=y_block_1,
                            x_period_1=x_period_1,
                            value_1=value_1,
                            alloc_data_dataset_name=alloc_data_dataset_name,
                            so_cell_table_name=so_cell_table_name
                        )
                        continue

                    # Lookup allocation_by_kr from map instead of querying
                    my_by_type = my_allocation_by_type_item.by_block_by_type
                    lookup_key = (my_from_type, my_to_type, my_by_type)
                    allocation_by_kr_item = allocation_by_kr_map.get(lookup_key)
                    
                    if allocation_by_kr_item is None:
                        print(f"[WARN] No allocation_by_kr_item found for key {lookup_key}, skipping")
                        continue
                    
                    print(f"[INFO] Found allocation_by_kr_item from map for key {lookup_key}: {allocation_by_kr_item}")

                    kr_block_3 = allocation_by_kr_item

                    # Step160: Batch query all by_percent values for all my_to_items
                    print(f"[INFO][Step 160] Building batch query for {len(my_to_items)} by_percent values")
                    to_item_values = [item.to_item for item in my_to_items]
                    
                    by_percent_batch_query = build_so_cell_by_kr_batch_query(
                        allocation_by_kr_item=kr_block_3,
                        to_items=to_item_values,
                        project_id=project_id,
                        dataset_id=alloc_data_dataset_name,
                        table_id=so_cell_table_name
                    )
                    
                    if by_percent_batch_query is None:
                        print(f"[WARN][Step 160] No valid by_percent query conditions, skipping")
                        continue
                    
                    print(f"[INFO][Step 160] Executing batch by_percent query")
                    by_percent_result_raw = bq.execute_query(by_percent_batch_query)
                    all_by_percent_items = SoCell.from_dataframe(by_percent_result_raw)
                    print(f"[INFO][Step 160] Batch query returned {len(all_by_percent_items)} by_percent items")
                    
                    # Group by_percent results by to_item
                    by_percent_map = group_by_percent_results(all_by_percent_items)
                    print(f"[INFO][Step 160] Grouped by_percent into {len(by_percent_map)} unique to_items")

                    # Step170: Process each my_to_item using the by_percent map
                    for my_to_item in my_to_items:
                        print(f"[INFO][Step 170] Start processing for each my_to_item: {my_to_item}")

                        # Lookup by_percent from map
                        by_percent = by_percent_map.get(my_to_item.to_item)
                        
                        if by_percent is None:
                            print(f"[WARN][Step 170] No by_percent found for to_item {my_to_item.to_item}, skipping")
                            continue
                        
                        print(f"[INFO][Step 170] Found by_percent from map: {by_percent} for to_item: {my_to_item.to_item}")

                        value_2 = value_1 * by_percent

                        if my_from_type == 'NP':
                            my_to_type_final = 'NP'
                            my_to_item_final = add_period_strings(x_period_1, my_to_item.to_item)
                            y_block_2 = copy.copy(y_block_1)
                            y_block_2.prev_ppc = x_period_1
                            y_block_2.now_np = my_to_item_final

                            insert_so_cell = create_socell_from_yblocks(
                                y_block_2=y_block_2,
                                y_block_1=y_block_1,
                                x_period_1=x_period_1,
                                value_2=value_2,
                                value_1=value_1,
                                by_type=my_by_type,
                                by_percent=by_percent,
                                to_alt=my_allocation_alt_item.to_alt
                            )

                            # Collect record for batch insert
                            batch_insert_records.append(insert_so_cell)
                            print(f"[INFO][Step 230] Added SoCell to batch: {insert_so_cell}")
            
            # Batch insert all collected records for this my_allocation_alt_item
            if batch_insert_records:
                print(f"[INFO][Step 240] Starting batch insert of {len(batch_insert_records)} records for z_number={my_allocation_alt_item.z_number}")
                success = bq.insert_rows_batch(
                    dataset_id=alloc_data_dataset_name,
                    table_id=so_cell_table_name,
                    rows_data=batch_insert_records
                )
                if success:
                    print(f"[INFO][Step 240] Successfully batch inserted {len(batch_insert_records)} SoCell records")
                else:
                    print(f"[ERROR][Step 240] Failed to batch insert {len(batch_insert_records)} SoCell records")
            else:
                print(f"[INFO][Step 240] No records to insert for z_number={my_allocation_alt_item.z_number}")
        
        print("[INFO] ================> DONE")

    except Exception as e:
        print(f"Lỗi: {str(e)}")
