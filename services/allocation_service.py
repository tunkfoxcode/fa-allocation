import copy
from utils.period_utils import add_period_with_offset
from services.so_cell_factory import create_socell_for_offset


def calculate_offset(
        bq,
        my_allocation_by_type_item,
        my_allocation_alt_item,
        y_block_1,
        x_period_1: str,
        value_1: float,
        alloc_data_dataset_name: str,
        so_cell_table_name: str
) -> bool:
    """
    Xử lý trường hợp offset: tính x_period_2, tạo SoCell và insert vào BigQuery

    Args:
        bq: BigQueryConnector instance
        my_allocation_by_type_item: AllocationByType item chứa offset trong by_block_by_type
        my_allocation_alt_item: AllocationALT item
        y_block_1: SoCell instance (YBlock1)
        x_period_1: XPeriod1
        value_1: Value1
        alloc_data_dataset_name: Dataset name
        so_cell_table_name: Table name

    Returns:
        True nếu insert thành công, False nếu thất bại
    """
    offset_month = int(my_allocation_by_type_item.by_block_by_type)

    x_period_2 = add_period_with_offset(x_period_1, offset_month)
    print(f"[INFO] Offset case: offset={offset_month}, x_period_1={x_period_1}, x_period_2={x_period_2}")

    insert_so_cell_offset = create_socell_for_offset(
        y_block_1=y_block_1,
        to_alt=my_allocation_alt_item.to_alt,
        x_period_2=x_period_2,
        x_period_1=x_period_1,
        value_1=value_1,
        by_type=my_allocation_by_type_item.by_block_by_type,
        z_number=my_allocation_alt_item.z_number
    )

    success = bq.insert_row(
        dataset_id=alloc_data_dataset_name,
        table_id=so_cell_table_name,
        row_data=insert_so_cell_offset
    )

    if success:
        print(f"[INFO] Offset case: Successfully inserted SoCell with x_period_2={x_period_2}")
    else:
        print(f"[ERROR] Offset case: Failed to insert SoCell")

    return success
