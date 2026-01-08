from dataclasses import dataclass
from typing import Optional, List


@dataclass
class SoCell:
    """
    Model class cho bảng so_cell_raw_full
    Mapping các field từ BigQuery sang Python object
    """
    i_o: Optional[str] = None
    z_block_zblock1_source: Optional[str] = None
    z_block_zblock1_pack: Optional[str] = None
    z_block_zblock1_scenario: Optional[str] = None
    z_block_zblock1_run: Optional[str] = None
    now_zblock2_alt: Optional[str] = None
    z_block_zblock2_file: Optional[str] = None
    z_block_zblock2_sheet: Optional[str] = None
    now_y_block_fnf_fnf: Optional[str] = None
    now_y_block_kr_item_code_kr1: Optional[str] = None
    now_y_block_kr_item_code_kr2: Optional[str] = None
    now_y_block_kr_item_code_kr3: Optional[str] = None
    now_y_block_kr_item_code_kr4: Optional[str] = None
    now_y_block_kr_item_code_kr5: Optional[str] = None
    now_y_block_kr_item_code_kr6: Optional[str] = None
    now_y_block_kr_item_code_kr7: Optional[str] = None
    now_y_block_kr_item_code_kr8: Optional[str] = None
    now_y_block_kr_item_name: Optional[str] = None
    now_y_block_cdt_cdt1: Optional[str] = None
    now_y_block_cdt_cdt2: Optional[str] = None
    now_y_block_cdt_cdt3: Optional[str] = None
    now_y_block_cdt_cdt4: Optional[str] = None
    now_y_block_ptnow_pt1: Optional[str] = None
    now_y_block_ptnow_pt2: Optional[str] = None
    now_y_block_ptnow_duration: Optional[str] = None
    now_y_block_ptprev_pt1: Optional[str] = None
    now_y_block_ptprev_pt2: Optional[str] = None
    now_y_block_ptprev_duration: Optional[str] = None
    now_y_block_ptfix_owntype: Optional[str] = None
    now_y_block_ptfix_aitype: Optional[str] = None
    now_y_block_ptsub_cty1: Optional[str] = None
    now_y_block_ptsub_cty2: Optional[str] = None
    now_y_block_ptsub_ostype: Optional[str] = None
    now_y_block_funnel_fu1: Optional[str] = None
    now_y_block_funnel_fu2: Optional[str] = None
    now_y_block_channel_ch: Optional[str] = None
    now_y_block_employee_egt1: Optional[str] = None
    now_y_block_employee_egt2: Optional[str] = None
    now_y_block_employee_egt3: Optional[str] = None
    now_y_block_employee_egt4: Optional[str] = None
    now_y_block_hr_hr1: Optional[str] = None
    now_y_block_hr_hr2: Optional[str] = None
    now_y_block_hr_hr3: Optional[str] = None
    now_y_block_sec: Optional[str] = None
    now_y_block_period_mx: Optional[str] = None
    now_y_block_period_dx: Optional[str] = None
    now_y_block_period_ppc: Optional[str] = None
    now_y_block_period_np: Optional[str] = None
    now_y_block_le_le1: Optional[str] = None
    now_y_block_le_le2: Optional[str] = None
    now_y_block_unit: Optional[str] = None
    uploaded_at: Optional[str] = None
    upload_batch_id: Optional[str] = None
    source_file: Optional[str] = None
    uploaded_by: Optional[str] = None
    source_row_no: Optional[int] = None
    so_row_id: Optional[str] = None
    now_np: Optional[str] = None
    now_value: Optional[float] = None
    time_col_name: Optional[str] = None
    prev_zblock2_alt: Optional[str] = None
    prev_y_block_fnf_fnf: Optional[str] = None
    prev_y_block_kr_item_code_kr1: Optional[str] = None
    prev_y_block_kr_item_code_kr2: Optional[str] = None
    prev_y_block_kr_item_code_kr3: Optional[str] = None
    prev_y_block_kr_item_code_kr4: Optional[str] = None
    prev_y_block_kr_item_code_kr5: Optional[str] = None
    prev_y_block_kr_item_code_kr6: Optional[str] = None
    prev_y_block_kr_item_code_kr7: Optional[str] = None
    prev_y_block_kr_item_code_kr8: Optional[str] = None
    prev_y_block_kr_item_name: Optional[str] = None
    prev_y_block_cdt_cdt1: Optional[str] = None
    prev_y_block_cdt_cdt2: Optional[str] = None
    prev_y_block_cdt_cdt3: Optional[str] = None
    prev_y_block_cdt_cdt4: Optional[str] = None
    prev_y_block_ptnow_pt1: Optional[str] = None
    prev_y_block_ptnow_pt2: Optional[str] = None
    prev_y_block_ptnow_duration: Optional[str] = None
    prev_y_block_ptprev_pt1: Optional[str] = None
    prev_y_block_ptprev_pt2: Optional[str] = None
    prev_y_block_ptprev_duration: Optional[str] = None
    prev_y_block_ptfix_owntype: Optional[str] = None
    prev_y_block_ptfix_aitype: Optional[str] = None
    prev_y_block_ptsub_cty1: Optional[str] = None
    prev_y_block_ptsub_cty2: Optional[str] = None
    prev_y_block_ptsub_ostype: Optional[str] = None
    prev_y_block_funnel_fu1: Optional[str] = None
    prev_y_block_funnel_fu2: Optional[str] = None
    prev_y_block_channel_ch: Optional[str] = None
    prev_y_block_employee_egt1: Optional[str] = None
    prev_y_block_employee_egt2: Optional[str] = None
    prev_y_block_employee_egt3: Optional[str] = None
    prev_y_block_employee_egt4: Optional[str] = None
    prev_y_block_hr_hr1: Optional[str] = None
    prev_y_block_hr_hr2: Optional[str] = None
    prev_y_block_hr_hr3: Optional[str] = None
    prev_y_block_sec: Optional[str] = None
    prev_y_block_period_mx: Optional[str] = None
    prev_y_block_period_dx: Optional[str] = None
    prev_y_block_period_ppc: Optional[str] = None
    prev_y_block_period_np: Optional[str] = None
    prev_y_block_le_le1: Optional[str] = None
    prev_y_block_le_le2: Optional[str] = None
    prev_y_block_unit: Optional[str] = None
    prev_ppc: Optional[str] = None
    prev_value: Optional[float] = None
    prev_np: Optional[str] = None
    by_block_bytype: Optional[str] = None
    by_block_bypercent: Optional[float] = None

    @classmethod
    def from_bigquery_row(cls, row) -> 'SoCell':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row

        return cls(
            i_o=data.get('i_o'),
            z_block_zblock1_source=data.get('z_block_zblock1_source'),
            z_block_zblock1_pack=data.get('Z-BLOCK_ZBlock1_PCK'),
            z_block_zblock1_scenario=data.get('z_block_zblock1_scenario'),
            z_block_zblock1_run=data.get('z_block_zblock1_run'),
            now_zblock2_alt=data.get('now_zblock2_alt'),
            z_block_zblock2_file=data.get('z_block_zblock2_file'),
            z_block_zblock2_sheet=data.get('z_block_zblock2_sheet'),
            now_y_block_fnf_fnf=data.get('now_y_block_fnf_fnf'),
            now_y_block_kr_item_code_kr1=data.get('now_y_block_kr_item_code_kr1'),
            now_y_block_kr_item_code_kr2=data.get('now_y_block_kr_item_code_kr2'),
            now_y_block_kr_item_code_kr3=data.get('now_y_block_kr_item_code_kr3'),
            now_y_block_kr_item_code_kr4=data.get('now_y_block_kr_item_code_kr4'),
            now_y_block_kr_item_code_kr5=data.get('now_y_block_kr_item_code_kr5'),
            now_y_block_kr_item_code_kr6=data.get('now_y_block_kr_item_code_kr6'),
            now_y_block_kr_item_code_kr7=data.get('now_y_block_kr_item_code_kr7'),
            now_y_block_kr_item_code_kr8=data.get('now_y_block_kr_item_code_kr8'),
            now_y_block_kr_item_name=data.get('now_y_block_kr_item_name'),
            now_y_block_cdt_cdt1=data.get('now_y_block_cdt_cdt1'),
            now_y_block_cdt_cdt2=data.get('now_y_block_cdt_cdt2'),
            now_y_block_cdt_cdt3=data.get('now_y_block_cdt_cdt3'),
            now_y_block_cdt_cdt4=data.get('now_y_block_cdt_cdt4'),
            now_y_block_ptnow_pt1=data.get('now_y_block_ptnow_pt1'),
            now_y_block_ptnow_pt2=data.get('now_y_block_ptnow_pt2'),
            now_y_block_ptnow_duration=data.get('now_y_block_ptnow_duration'),
            now_y_block_ptprev_pt1=data.get('now_y_block_ptprev_pt1'),
            now_y_block_ptprev_pt2=data.get('now_y_block_ptprev_pt2'),
            now_y_block_ptprev_duration=data.get('now_y_block_ptprev_duration'),
            now_y_block_ptfix_owntype=data.get('now_y_block_ptfix_owntype'),
            now_y_block_ptfix_aitype=data.get('now_y_block_ptfix_aitype'),
            now_y_block_ptsub_cty1=data.get('now_y_block_ptsub_cty1'),
            now_y_block_ptsub_cty2=data.get('now_y_block_ptsub_cty2'),
            now_y_block_ptsub_ostype=data.get('now_y_block_ptsub_ostype'),
            now_y_block_funnel_fu1=data.get('now_y_block_funnel_fu1'),
            now_y_block_funnel_fu2=data.get('now_y_block_funnel_fu2'),
            now_y_block_channel_ch=data.get('now_y_block_channel_ch'),
            now_y_block_employee_egt1=data.get('now_y_block_employee_egt1'),
            now_y_block_employee_egt2=data.get('now_y_block_employee_egt2'),
            now_y_block_employee_egt3=data.get('now_y_block_employee_egt3'),
            now_y_block_employee_egt4=data.get('now_y_block_employee_egt4'),
            now_y_block_hr_hr1=data.get('now_y_block_hr_hr1'),
            now_y_block_hr_hr2=data.get('now_y_block_hr_hr2'),
            now_y_block_hr_hr3=data.get('now_y_block_hr_hr3'),
            now_y_block_sec=data.get('now_y_block_sec'),
            now_y_block_period_mx=data.get('now_y_block_period_mx'),
            now_y_block_period_dx=data.get('now_y_block_period_dx'),
            now_y_block_period_ppc=data.get('now_y_block_period_ppc'),
            now_y_block_period_np=data.get('now_y_block_period_np'),
            now_y_block_le_le1=data.get('now_y_block_le_le1'),
            now_y_block_le_le2=data.get('now_y_block_le_le2'),
            now_y_block_unit=data.get('now_y_block_unit'),
            uploaded_at=data.get('uploaded_at'),
            upload_batch_id=data.get('upload_batch_id'),
            source_file=data.get('source_file'),
            uploaded_by=data.get('uploaded_by'),
            source_row_no=data.get('source_row_no'),
            so_row_id=data.get('so_row_id'),
            now_np=data.get('now_np'),
            now_value=data.get('now_value'),
            time_col_name=data.get('time_col_name'),
            prev_zblock2_alt=data.get('prev_zblock2_alt'),
            prev_y_block_fnf_fnf=data.get('prev_y_block_fnf_fnf'),
            prev_y_block_kr_item_code_kr1=data.get('prev_y_block_kr_item_code_kr1'),
            prev_y_block_kr_item_code_kr2=data.get('prev_y_block_kr_item_code_kr2'),
            prev_y_block_kr_item_code_kr3=data.get('prev_y_block_kr_item_code_kr3'),
            prev_y_block_kr_item_code_kr4=data.get('prev_y_block_kr_item_code_kr4'),
            prev_y_block_kr_item_code_kr5=data.get('prev_y_block_kr_item_code_kr5'),
            prev_y_block_kr_item_code_kr6=data.get('prev_y_block_kr_item_code_kr6'),
            prev_y_block_kr_item_code_kr7=data.get('prev_y_block_kr_item_code_kr7'),
            prev_y_block_kr_item_code_kr8=data.get('prev_y_block_kr_item_code_kr8'),
            prev_y_block_kr_item_name=data.get('prev_y_block_kr_item_name'),
            prev_y_block_cdt_cdt1=data.get('prev_y_block_cdt_cdt1'),
            prev_y_block_cdt_cdt2=data.get('prev_y_block_cdt_cdt2'),
            prev_y_block_cdt_cdt3=data.get('prev_y_block_cdt_cdt3'),
            prev_y_block_cdt_cdt4=data.get('prev_y_block_cdt_cdt4'),
            prev_y_block_ptnow_pt1=data.get('prev_y_block_ptnow_pt1'),
            prev_y_block_ptnow_pt2=data.get('prev_y_block_ptnow_pt2'),
            prev_y_block_ptnow_duration=data.get('prev_y_block_ptnow_duration'),
            prev_y_block_ptprev_pt1=data.get('prev_y_block_ptprev_pt1'),
            prev_y_block_ptprev_pt2=data.get('prev_y_block_ptprev_pt2'),
            prev_y_block_ptprev_duration=data.get('prev_y_block_ptprev_duration'),
            prev_y_block_ptfix_owntype=data.get('prev_y_block_ptfix_owntype'),
            prev_y_block_ptfix_aitype=data.get('prev_y_block_ptfix_aitype'),
            prev_y_block_ptsub_cty1=data.get('prev_y_block_ptsub_cty1'),
            prev_y_block_ptsub_cty2=data.get('prev_y_block_ptsub_cty2'),
            prev_y_block_ptsub_ostype=data.get('prev_y_block_ptsub_ostype'),
            prev_y_block_funnel_fu1=data.get('prev_y_block_funnel_fu1'),
            prev_y_block_funnel_fu2=data.get('prev_y_block_funnel_fu2'),
            prev_y_block_channel_ch=data.get('prev_y_block_channel_ch'),
            prev_y_block_employee_egt1=data.get('prev_y_block_employee_egt1'),
            prev_y_block_employee_egt2=data.get('prev_y_block_employee_egt2'),
            prev_y_block_employee_egt3=data.get('prev_y_block_employee_egt3'),
            prev_y_block_employee_egt4=data.get('prev_y_block_employee_egt4'),
            prev_y_block_hr_hr1=data.get('prev_y_block_hr_hr1'),
            prev_y_block_hr_hr2=data.get('prev_y_block_hr_hr2'),
            prev_y_block_hr_hr3=data.get('prev_y_block_hr_hr3'),
            prev_y_block_sec=data.get('prev_y_block_sec'),
            prev_y_block_period_mx=data.get('prev_y_block_period_mx'),
            prev_y_block_period_dx=data.get('prev_y_block_period_dx'),
            prev_y_block_period_ppc=data.get('prev_y_block_period_ppc'),
            prev_y_block_period_np=data.get('prev_y_block_period_np'),
            prev_y_block_le_le1=data.get('prev_y_block_le_le1'),
            prev_y_block_le_le2=data.get('prev_y_block_le_le2'),
            prev_y_block_unit=data.get('prev_y_block_unit'),
            prev_ppc=data.get('prev_ppc'),
            prev_value=data.get('prev_value'),
            prev_np=data.get('prev_np'),
            by_block_bytype=data.get('by_block_bytype'),
            by_block_bypercent=data.get('by_block_bypercent')
        )

    @classmethod
    def from_dataframe(cls, df) -> List['SoCell']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]

    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"SoCellRawFull(fnf='{self.now_y_block_fnf_fnf}', "
                f"kr1='{self.now_y_block_kr_item_code_kr1}', "
                f"now_value={self.now_value}, by_type='{self.by_block_bytype}')")
