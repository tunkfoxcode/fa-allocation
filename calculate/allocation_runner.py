import copy
from db.bigquery_connector import BigQueryConnector
from models.allocation_models import AllocationALT, AllocationToItem, AllocationByType, AllocationByKR
from models.so_cell_model import SoCell
from queries.query_builder import build_so_cell_query, build_so_cell_by_kr_query, build_so_cell_prev_query
from utils.period_utils import add_period_strings
from services.allocation_service import calculate_offset
from services.so_cell_factory import create_socell_from_yblocks


def run_allocate():
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
        ORDER BY ZNumber ASC
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

            query_to_item = f"""
            SELECT * 
            FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
            WHERE TO_Y_BLOCK_ToType = "{my_allocation_alt_item.to_type}"
            """
            my_to_items_raw = bq.execute_query(query_to_item)
            my_to_items = AllocationToItem.from_dataframe(my_to_items_raw)

            print(f"[INFO][Step 40] We having {len(my_to_items)} my_to_items by query: \n {query_to_item}")

            query_by_type = f"""
            SELECT * 
            FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_type_table_name}` 
            WHERE ZNumber = {my_allocation_alt_item.z_number}
            ORDER BY YNumber DESC
            """

            my_by_type_raw = bq.execute_query(query_by_type)
            my_allocation_by_type_items = AllocationByType.from_dataframe(my_by_type_raw)

            print(
                f"[INFO][Step 50] We having {len(my_allocation_by_type_items)} my_allocation_by_type_items by query: \n {query_by_type}")

            for my_allocation_by_type_item in my_allocation_by_type_items:
                print(f"[INFO][Step 60] Processing for each my_allocation_by_type_item: {my_allocation_by_type_item}")

                if my_allocation_by_type_item.by_block_by_type == 'GAgg':
                    print(
                        "INFO][Step 60] Ignore process for this my_allocation_by_type_item because by type is GAgg or ByAgg")
                    continue

                if my_allocation_by_type_item.by_block_by_type == 'ByAgg':
                    query_pt_s2 = f"""
                    SELECT TO_Y_BLOCK_ToItem 
                    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
                    WHERE TO_Y_BLOCK_ToType = 'PT-S2'
                    """
                    pt_s2_raw = bq.execute_query(query_pt_s2)

                    pt_s2_items = [
                        str(item) for item in pt_s2_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()
                    ]

                    query_cty_s2 = f"""
                                        SELECT TO_Y_BLOCK_ToItem 
                                        FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
                                        WHERE TO_Y_BLOCK_ToType = 'CTY-S2'
                                        """
                    cty_s2_raw = bq.execute_query(query_cty_s2)
                    cty_s2_items = [
                        str(item) for item in cty_s2_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()
                    ]

                    query_np_d365 = f"""
                                                            SELECT TO_Y_BLOCK_ToItem 
                                                            FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
                                                            WHERE TO_Y_BLOCK_ToType = 'NP-D365'
                                                            """
                    np_d365_raw = bq.execute_query(query_np_d365)
                    np_d365_items = [
                        str(item) for item in np_d365_raw['TO_Y_BLOCK_ToItem'].dropna().tolist()
                    ]
                    for my_to_item_pts2 in pt_s2_items:
                        for my_to_item_ctys2 in cty_s2_items:
                            for my_to_item_np_d365 in np_d365_items:
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

                                value_2 = 0

                                for so_cell_byagg_item in so_cell_byagg_items:
                                    value_1 = so_cell_byagg_item.now_value
                                    if value_1 is None:
                                        continue
                                    value_2 = value_2 + value_1

                                pt1 = my_to_item_pts2[:2] if len(my_to_item_pts2) >= 2 else my_to_item_pts2
                                pt2 = my_to_item_pts2[2:] if len(my_to_item_pts2) > 2 else None

                                cty1 = my_to_item_ctys2[:2] if len(my_to_item_ctys2) >= 2 else my_to_item_ctys2
                                cty2 = my_to_item_ctys2[2:] if len(my_to_item_ctys2) > 2 else None

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

                                success = bq.insert_row(
                                    dataset_id=alloc_data_dataset_name,
                                    table_id=so_cell_table_name,
                                    row_data=insert_so_cell_byagg
                                )
                                if success:
                                    print(
                                        f"[INFO] ByAgg: Successfully inserted aggregated SoCell with value={value_2}, PTS2={my_to_item_pts2}, CTYS2={my_to_item_ctys2}, NPD365={my_to_item_np_d365}")
                                else:
                                    print(f"[ERROR] ByAgg: Failed to insert aggregated SoCell")

                    continue

                query_so_cell = build_so_cell_query(
                    my_allocation_by_type_item,
                    project_id,
                    dataset_id=alloc_data_dataset_name,
                    table_id=so_cell_table_name
                )
                my_so_cell_raw = bq.execute_query(query_so_cell)

                from_so_cell_items = SoCell.from_dataframe(my_so_cell_raw)
                print(
                    f"[INFO][Step 70] We having {len(from_so_cell_items)} from_so_cell_items by query: \n {query_so_cell}")

                for from_so_cell_item in from_so_cell_items:
                    print(f"[INFO][Step 80] Start processing for each from_so_cell_item: {from_so_cell_item}")
                    y_block_1 = from_so_cell_item
                    x_period_1 = from_so_cell_item.now_np
                    value_1 = from_so_cell_item.now_value
                    print(
                        f"[INFO][Step 80] We have y_block_1: {y_block_1}, x_period_1: {x_period_1}, value_1: {value_1}")

                    query_so_cell_prev = build_so_cell_prev_query(
                        y_block_1=y_block_1,
                        x_period_1=x_period_1,
                        z_number=my_allocation_alt_item.z_number,
                        project_id=project_id,
                        dataset_id=alloc_data_dataset_name,
                        table_id=so_cell_table_name
                    )
                    so_cell_raw = bq.execute_query(query_so_cell_prev)
                    so_cells_prev_y_block = SoCell.from_dataframe(so_cell_raw)
                    print(
                        f"[INFO][Step 110] We having {len(so_cells_prev_y_block)} so_cells_prev_y_block by query: \n {query_so_cell_prev}")

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

                    my_from_type = my_allocation_alt_item.from_type
                    my_to_type = my_allocation_alt_item.to_type
                    my_by_type = my_allocation_by_type_item.by_block_by_type

                    query_allocation_by_kr = f"""
                    SELECT * 
                    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_kr_table_name}` 
                    WHERE TO_Y_BLOCK_KR6 = '{my_from_type}'
                    AND TO_Y_BLOCK_KR4 = '{my_to_type}'
                    AND BY_BLOCK_ByType = '{my_by_type}'
                    """
                    allocation_by_kr_raw = bq.execute_query(query_allocation_by_kr)
                    allocation_by_kr_items = AllocationByKR.from_dataframe(allocation_by_kr_raw)

                    print(
                        f"[INFO] We having {len(allocation_by_kr_items)} allocation_by_kr_items by query: \n {query_allocation_by_kr}")

                    if len(allocation_by_kr_items) < 0:
                        print("[WARN] Skip process because allocation_by_kr_items is empty")
                        continue
                    allocation_by_kr_item = allocation_by_kr_items[0]
                    print(
                        f"[INFO] We have allocation_by_kr_item by picking the first element of allocation_by_kr_items: {allocation_by_kr_item}")

                    kr_block_3 = allocation_by_kr_item

                    for my_to_item in my_to_items:
                        print(f"[INFO] Start processing for each my_to_item: {my_to_item}")

                        filter_block_3 = my_to_item

                        by_percent_query = build_so_cell_by_kr_query(
                            allocation_by_kr_item=kr_block_3,
                            allocation_to_item=filter_block_3,
                            project_id=project_id,
                            to_item=my_to_item.to_item,
                            dataset_id=alloc_data_dataset_name,
                            table_id=so_cell_table_name
                        )
                        by_percent_result_raw = bq.execute_query(by_percent_query)
                        by_percent_items = SoCell.from_dataframe(by_percent_result_raw)
                        print(
                            f"[INFO][Step 170] We having {len(by_percent_items)} by_percent by query: \n {by_percent_query}")
                        if len(by_percent_items) == 0:
                            print("[WARN][Step 170] Skip process because by_percent_items is empty")
                            continue
                        by_percent = by_percent_items[0].now_value
                        if by_percent is None:
                            continue
                        print(
                            f"[INFO][Step 170] We have by_percent: \n {by_percent} \n by kr_block_3: {kr_block_3} and filter_block_3: {filter_block_3}")

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

                            success = bq.insert_row(
                                dataset_id=alloc_data_dataset_name,
                                table_id=so_cell_table_name,
                                row_data=insert_so_cell
                            )
                            if success:
                                print(f"[INFO][Step 230] Successfully inserted SoCell: {insert_so_cell}")
                            else:
                                print(f"[ERROR][Step 230] Failed to insert SoCell: {insert_so_cell}")
        print("[INFO] ================> DONE")

    except Exception as e:
        print(f"Lỗi: {str(e)}")
