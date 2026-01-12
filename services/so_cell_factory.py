import copy
from models.so_cell_model import SoCell


def create_socell_from_yblocks(
        y_block_2: SoCell,
        y_block_1: SoCell,
        x_period_1: str,
        value_2: float,
        value_1: float,
        by_type: str,
        by_percent: float,
        to_alt: str
) -> SoCell:
    """
    Tạo SoCell instance từ 2 YBlocks

    Args:
        y_block_2: SoCell instance (YBlock2)
        y_block_1: SoCell instance (YBlock1)
        x_period_1: XPeriod1
        value_2: Value2
        value_1: Value1
        by_type: ByType
        by_percent: ByPercent
        to_alt: ToALT

    Returns:
        SoCell instance mới
    """
    return SoCell(
        now_y_block_fnf_fnf=y_block_2.now_y_block_fnf_fnf,
        now_y_block_kr_item_code_kr1=y_block_2.now_y_block_kr_item_code_kr1,
        now_y_block_kr_item_code_kr2=y_block_2.now_y_block_kr_item_code_kr2,
        now_y_block_kr_item_code_kr3=y_block_2.now_y_block_kr_item_code_kr3,
        now_y_block_kr_item_code_kr4=y_block_2.now_y_block_kr_item_code_kr4,
        now_y_block_kr_item_code_kr5=y_block_2.now_y_block_kr_item_code_kr5,
        now_y_block_kr_item_code_kr6=y_block_2.now_y_block_kr_item_code_kr6,
        now_y_block_kr_item_code_kr7=y_block_2.now_y_block_kr_item_code_kr7,
        now_y_block_kr_item_code_kr8=y_block_2.now_y_block_kr_item_code_kr8,
        now_y_block_cdt_cdt1=y_block_2.now_y_block_cdt_cdt1,
        now_y_block_cdt_cdt2=y_block_2.now_y_block_cdt_cdt2,
        now_y_block_cdt_cdt3=y_block_2.now_y_block_cdt_cdt3,
        now_y_block_cdt_cdt4=y_block_2.now_y_block_cdt_cdt4,
        now_y_block_ptnow_pt1=y_block_2.now_y_block_ptnow_pt1,
        now_y_block_ptnow_pt2=y_block_2.now_y_block_ptnow_pt2,
        now_y_block_ptnow_duration=y_block_2.now_y_block_ptnow_duration,
        now_y_block_ptprev_pt1=y_block_2.now_y_block_ptprev_pt1,
        now_y_block_ptprev_pt2=y_block_2.now_y_block_ptprev_pt2,
        now_y_block_ptprev_duration=y_block_2.now_y_block_ptprev_duration,
        now_y_block_ptfix_owntype=y_block_2.now_y_block_ptfix_owntype,
        now_y_block_ptfix_aitype=y_block_2.now_y_block_ptfix_aitype,
        now_y_block_ptsub_cty1=y_block_2.now_y_block_ptsub_cty1,
        now_y_block_ptsub_cty2=y_block_2.now_y_block_ptsub_cty2,
        now_y_block_ptsub_ostype=y_block_2.now_y_block_ptsub_ostype,
        now_y_block_funnel_fu1=y_block_2.now_y_block_funnel_fu1,
        now_y_block_funnel_fu2=y_block_2.now_y_block_funnel_fu2,
        now_y_block_channel_ch=y_block_2.now_y_block_channel_ch,
        now_y_block_employee_egt1=y_block_2.now_y_block_employee_egt1,
        now_y_block_employee_egt2=y_block_2.now_y_block_employee_egt2,
        now_y_block_employee_egt3=y_block_2.now_y_block_employee_egt3,
        now_y_block_employee_egt4=y_block_2.now_y_block_employee_egt4,
        now_y_block_hr_hr1=y_block_2.now_y_block_hr_hr1,
        now_y_block_hr_hr2=y_block_2.now_y_block_hr_hr2,
        now_y_block_hr_hr3=y_block_2.now_y_block_hr_hr3,
        now_y_block_sec=y_block_2.now_y_block_sec,
        now_y_block_period_mx=y_block_2.now_y_block_period_mx,
        now_y_block_period_dx=y_block_2.now_y_block_period_dx,
        now_y_block_period_ppc=y_block_2.now_y_block_period_ppc,
        now_y_block_period_np=y_block_2.now_y_block_period_np,
        now_y_block_le_le1=y_block_2.now_y_block_le_le1,
        now_y_block_le_le2=y_block_2.now_y_block_le_le2,
        now_y_block_unit=y_block_2.now_y_block_unit,
        now_np=y_block_2.now_np,
        now_value=value_2,
        prev_y_block_kr_item_code_kr1=y_block_1.now_y_block_kr_item_code_kr1,
        prev_y_block_kr_item_code_kr2=y_block_1.now_y_block_kr_item_code_kr2,
        prev_y_block_kr_item_code_kr3=y_block_1.now_y_block_kr_item_code_kr3,
        prev_y_block_kr_item_code_kr4=y_block_1.now_y_block_kr_item_code_kr4,
        prev_y_block_kr_item_code_kr5=y_block_1.now_y_block_kr_item_code_kr5,
        prev_y_block_kr_item_code_kr6=y_block_1.now_y_block_kr_item_code_kr6,
        prev_y_block_kr_item_code_kr7=y_block_1.now_y_block_kr_item_code_kr7,
        prev_y_block_kr_item_code_kr8=y_block_1.now_y_block_kr_item_code_kr8,
        prev_y_block_cdt_cdt1=y_block_1.now_y_block_cdt_cdt1,
        prev_y_block_cdt_cdt2=y_block_1.now_y_block_cdt_cdt2,
        prev_y_block_cdt_cdt3=y_block_1.now_y_block_cdt_cdt3,
        prev_y_block_cdt_cdt4=y_block_1.now_y_block_cdt_cdt4,
        prev_y_block_ptnow_pt1=y_block_1.now_y_block_ptnow_pt1,
        prev_y_block_ptnow_pt2=y_block_1.now_y_block_ptnow_pt2,
        prev_y_block_ptnow_duration=y_block_1.now_y_block_ptnow_duration,
        prev_y_block_ptprev_pt1=y_block_1.now_y_block_ptprev_pt1,
        prev_y_block_ptprev_pt2=y_block_1.now_y_block_ptprev_pt2,
        prev_y_block_ptprev_duration=y_block_1.now_y_block_ptprev_duration,
        prev_y_block_ptfix_owntype=y_block_1.now_y_block_ptfix_owntype,
        prev_y_block_ptfix_aitype=y_block_1.now_y_block_ptfix_aitype,
        prev_y_block_ptsub_cty1=y_block_1.now_y_block_ptsub_cty1,
        prev_y_block_ptsub_cty2=y_block_1.now_y_block_ptsub_cty2,
        prev_y_block_ptsub_ostype=y_block_1.now_y_block_ptsub_ostype,
        prev_y_block_funnel_fu1=y_block_1.now_y_block_funnel_fu1,
        prev_y_block_funnel_fu2=y_block_1.now_y_block_funnel_fu2,
        prev_y_block_channel_ch=y_block_1.now_y_block_channel_ch,
        prev_y_block_employee_egt1=y_block_1.now_y_block_employee_egt1,
        prev_y_block_employee_egt2=y_block_1.now_y_block_employee_egt2,
        prev_y_block_employee_egt3=y_block_1.now_y_block_employee_egt3,
        prev_y_block_employee_egt4=y_block_1.now_y_block_employee_egt4,
        prev_y_block_hr_hr1=y_block_1.now_y_block_hr_hr1,
        prev_y_block_hr_hr2=y_block_1.now_y_block_hr_hr2,
        prev_y_block_hr_hr3=y_block_1.now_y_block_hr_hr3,
        prev_y_block_sec=y_block_1.now_y_block_sec,
        prev_y_block_period_mx=y_block_1.now_y_block_period_mx,
        prev_y_block_period_dx=y_block_1.now_y_block_period_dx,
        prev_y_block_le_le1=y_block_1.now_y_block_le_le1,
        prev_y_block_le_le2=y_block_1.now_y_block_le_le2,
        prev_y_block_unit=y_block_1.now_y_block_unit,
        prev_y_block_fnf_fnf=y_block_1.now_y_block_fnf_fnf,
        prev_y_block_kr_item_name=y_block_1.now_y_block_kr_item_name,
        prev_y_block_period_np=x_period_1,
        prev_value=value_1,
        by_block_bytype=by_type,
        by_block_bypercent=by_percent,
        now_zblock2_alt=to_alt,
        prev_zblock2_alt=y_block_1.now_zblock2_alt,
        prev_np=x_period_1
    )


def create_socell_for_offset(
        y_block_1: SoCell,
        to_alt: str,
        x_period_2: str,
        x_period_1: str,
        value_1: float,
        by_type: str,
        z_number: int
) -> SoCell:
    """
    Tạo SoCell instance cho trường hợp offset (NowYBlock = PrevYBlock = YBlock1)

    Args:
        y_block_1: SoCell instance (YBlock1)
        to_alt: MyToALT
        x_period_2: XPeriod2 (calculated from offset)
        x_period_1: XPeriod1 (original)
        value_1: Value1
        by_type: MyAllocationByTypeItem.ByType
        z_number: MyAllocationALTItem.ZNumber

    Returns:
        SoCell instance mới
    """
    return SoCell(
        now_y_block_kr_item_code_kr1=y_block_1.now_y_block_kr_item_code_kr1,
        now_y_block_kr_item_code_kr2=y_block_1.now_y_block_kr_item_code_kr2,
        now_y_block_kr_item_code_kr3=y_block_1.now_y_block_kr_item_code_kr3,
        now_y_block_kr_item_code_kr4=y_block_1.now_y_block_kr_item_code_kr4,
        now_y_block_kr_item_code_kr5=y_block_1.now_y_block_kr_item_code_kr5,
        now_y_block_kr_item_code_kr6=y_block_1.now_y_block_kr_item_code_kr6,
        now_y_block_kr_item_code_kr7=y_block_1.now_y_block_kr_item_code_kr7,
        now_y_block_kr_item_code_kr8=y_block_1.now_y_block_kr_item_code_kr8,
        now_y_block_cdt_cdt1=y_block_1.now_y_block_cdt_cdt1,
        now_y_block_cdt_cdt2=y_block_1.now_y_block_cdt_cdt2,
        now_y_block_cdt_cdt3=y_block_1.now_y_block_cdt_cdt3,
        now_y_block_cdt_cdt4=y_block_1.now_y_block_cdt_cdt4,
        now_y_block_ptnow_pt1=y_block_1.now_y_block_ptnow_pt1,
        now_y_block_ptnow_pt2=y_block_1.now_y_block_ptnow_pt2,
        now_y_block_ptnow_duration=y_block_1.now_y_block_ptnow_duration,
        now_y_block_ptprev_pt1=y_block_1.now_y_block_ptprev_pt1,
        now_y_block_ptprev_pt2=y_block_1.now_y_block_ptprev_pt2,
        now_y_block_ptprev_duration=y_block_1.now_y_block_ptprev_duration,
        now_y_block_ptfix_owntype=y_block_1.now_y_block_ptfix_owntype,
        now_y_block_ptfix_aitype=y_block_1.now_y_block_ptfix_aitype,
        now_y_block_ptsub_cty1=y_block_1.now_y_block_ptsub_cty1,
        now_y_block_ptsub_cty2=y_block_1.now_y_block_ptsub_cty2,
        now_y_block_ptsub_ostype=y_block_1.now_y_block_ptsub_ostype,
        now_y_block_funnel_fu1=y_block_1.now_y_block_funnel_fu1,
        now_y_block_funnel_fu2=y_block_1.now_y_block_funnel_fu2,
        now_y_block_channel_ch=y_block_1.now_y_block_channel_ch,
        now_y_block_employee_egt1=y_block_1.now_y_block_employee_egt1,
        now_y_block_employee_egt2=y_block_1.now_y_block_employee_egt2,
        now_y_block_employee_egt3=y_block_1.now_y_block_employee_egt3,
        now_y_block_employee_egt4=y_block_1.now_y_block_employee_egt4,
        now_y_block_hr_hr1=y_block_1.now_y_block_hr_hr1,
        now_y_block_hr_hr2=y_block_1.now_y_block_hr_hr2,
        now_y_block_hr_hr3=y_block_1.now_y_block_hr_hr3,
        now_y_block_sec=y_block_1.now_y_block_sec,
        now_y_block_period_mx=y_block_1.now_y_block_period_mx,
        now_y_block_period_dx=y_block_1.now_y_block_period_dx,
        now_y_block_period_ppc=y_block_1.now_y_block_period_ppc,
        now_y_block_period_np=y_block_1.now_y_block_period_np,
        now_y_block_le_le1=y_block_1.now_y_block_le_le1,
        now_y_block_le_le2=y_block_1.now_y_block_le_le2,
        now_y_block_unit=y_block_1.now_y_block_unit,
        now_zblock2_alt=to_alt,
        now_np=x_period_2,
        now_value=value_1,
        prev_y_block_kr_item_code_kr1=y_block_1.now_y_block_kr_item_code_kr1,
        prev_y_block_kr_item_code_kr2=y_block_1.now_y_block_kr_item_code_kr2,
        prev_y_block_kr_item_code_kr3=y_block_1.now_y_block_kr_item_code_kr3,
        prev_y_block_kr_item_code_kr4=y_block_1.now_y_block_kr_item_code_kr4,
        prev_y_block_kr_item_code_kr5=y_block_1.now_y_block_kr_item_code_kr5,
        prev_y_block_kr_item_code_kr6=y_block_1.now_y_block_kr_item_code_kr6,
        prev_y_block_kr_item_code_kr7=y_block_1.now_y_block_kr_item_code_kr7,
        prev_y_block_kr_item_code_kr8=y_block_1.now_y_block_kr_item_code_kr8,
        prev_y_block_cdt_cdt1=y_block_1.now_y_block_cdt_cdt1,
        prev_y_block_cdt_cdt2=y_block_1.now_y_block_cdt_cdt2,
        prev_y_block_cdt_cdt3=y_block_1.now_y_block_cdt_cdt3,
        prev_y_block_cdt_cdt4=y_block_1.now_y_block_cdt_cdt4,
        prev_y_block_ptnow_pt1=y_block_1.now_y_block_ptnow_pt1,
        prev_y_block_ptnow_pt2=y_block_1.now_y_block_ptnow_pt2,
        prev_y_block_ptnow_duration=y_block_1.now_y_block_ptnow_duration,
        prev_y_block_ptprev_pt1=y_block_1.now_y_block_ptprev_pt1,
        prev_y_block_ptprev_pt2=y_block_1.now_y_block_ptprev_pt2,
        prev_y_block_ptprev_duration=y_block_1.now_y_block_ptprev_duration,
        prev_y_block_ptfix_owntype=y_block_1.now_y_block_ptfix_owntype,
        prev_y_block_ptfix_aitype=y_block_1.now_y_block_ptfix_aitype,
        prev_y_block_ptsub_cty1=y_block_1.now_y_block_ptsub_cty1,
        prev_y_block_ptsub_cty2=y_block_1.now_y_block_ptsub_cty2,
        prev_y_block_ptsub_ostype=y_block_1.now_y_block_ptsub_ostype,
        prev_y_block_funnel_fu1=y_block_1.now_y_block_funnel_fu1,
        prev_y_block_funnel_fu2=y_block_1.now_y_block_funnel_fu2,
        prev_y_block_channel_ch=y_block_1.now_y_block_channel_ch,
        prev_y_block_employee_egt1=y_block_1.now_y_block_employee_egt1,
        prev_y_block_employee_egt2=y_block_1.now_y_block_employee_egt2,
        prev_y_block_employee_egt3=y_block_1.now_y_block_employee_egt3,
        prev_y_block_employee_egt4=y_block_1.now_y_block_employee_egt4,
        prev_y_block_hr_hr1=y_block_1.now_y_block_hr_hr1,
        prev_y_block_hr_hr2=y_block_1.now_y_block_hr_hr2,
        prev_y_block_hr_hr3=y_block_1.now_y_block_hr_hr3,
        prev_y_block_sec=y_block_1.now_y_block_sec,
        prev_y_block_period_mx=y_block_1.now_y_block_period_mx,
        prev_y_block_period_dx=y_block_1.now_y_block_period_dx,
        prev_y_block_period_np=y_block_1.now_y_block_period_np,
        prev_y_block_le_le1=y_block_1.now_y_block_le_le1,
        prev_y_block_le_le2=y_block_1.now_y_block_le_le2,
        prev_y_block_unit=y_block_1.now_y_block_unit,
        prev_ppc=x_period_1,
        prev_value=value_1,
        by_block_bytype=by_type,
    )
