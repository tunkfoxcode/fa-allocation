# ============================================================================
# GOOGLE COLAB - FINANCE ALLOCATION BIGQUERY
# ============================================================================
# Để chạy trên Google Colab, không cần OAuth2 credentials file
# Colab sẽ tự động authenticate với Google Cloud
# ============================================================================

# BLOCK 1: Import Libraries
# ----------------------------------------------------------------------------
from dataclasses import dataclass
from typing import Optional, List
from google.cloud import bigquery
import pandas as pd

# BLOCK 2: Authenticate với BigQuery (chỉ cần chạy 1 lần)
# ----------------------------------------------------------------------------
# Uncomment dòng dưới khi chạy trên Colab để authenticate
# from google.colab import auth
# auth.authenticate_user()

# BLOCK 3: Model Classes - AllocationALT
# ----------------------------------------------------------------------------
@dataclass
class AllocationALT:
    """
    Model class cho bảng AllocationALT_NativeTable
    Mapping các field từ BigQuery sang Python object
    """
    z_number: Optional[int] = None
    from_alt: Optional[str] = None
    to_alt: Optional[str] = None
    from_type: Optional[str] = None
    to_type: Optional[str] = None
    
    @classmethod
    def from_bigquery_row(cls, row) -> 'AllocationALT':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row
            
        return cls(
            z_number=data.get('ZNumber'),
            from_alt=data.get('FROM_ALT_FromALT'),
            to_alt=data.get('TO_ALT_ToALT'),
            from_type=data.get('FROM_Y_BLOCK_FromType'),
            to_type=data.get('TO_Y_BLOCK_ToType')
        )
    
    @classmethod
    def from_dataframe(cls, df) -> List['AllocationALT']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [
            cls(
                z_number=row.get('ZNumber'),
                from_alt=row.get('FROM_ALT_FromALT'),
                to_alt=row.get('TO_ALT_ToALT'),
                from_type=row.get('FROM_Y_BLOCK_FromType'),
                to_type=row.get('TO_Y_BLOCK_ToType')
            )
            for _, row in df.iterrows()
        ]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"AllocationALT(z_number={self.z_number}, "
                f"from_alt='{self.from_alt}', to_alt='{self.to_alt}', "
                f"from_type='{self.from_type}', to_type='{self.to_type}')")


# BLOCK 4: Model Classes - AllocationToItem
# ----------------------------------------------------------------------------
@dataclass
class AllocationToItem:
    """
    Model class cho bảng AllocationToItem
    Mapping các field từ BigQuery sang Python object
    """
    from_type: Optional[str] = None
    from_item: Optional[int] = None
    to_type: Optional[str] = None
    to_item: Optional[str] = None
    config_upload_at: Optional[int] = None
    
    @classmethod
    def from_bigquery_row(cls, row) -> 'AllocationToItem':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row
            
        return cls(
            from_type=data.get('FROM_Y_BLOCK_FromType'),
            from_item=data.get('FROM_Y_BLOCK_FromItem'),
            to_type=data.get('TO_Y_BLOCK_ToType'),
            to_item=data.get('TO_Y_BLOCK_ToItem'),
            config_upload_at=data.get('Config_Upload_at')
        )
    
    @classmethod
    def from_dataframe(cls, df) -> List['AllocationToItem']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [
            cls(
                from_type=row.get('FROM_Y_BLOCK_FromType'),
                from_item=row.get('FROM_Y_BLOCK_FromItem'),
                to_type=row.get('TO_Y_BLOCK_ToType'),
                to_item=row.get('TO_Y_BLOCK_ToItem'),
                config_upload_at=row.get('Config_Upload_at')
            )
            for _, row in df.iterrows()
        ]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"AllocationToItem(from_type='{self.from_type}', "
                f"from_item={self.from_item}, to_type='{self.to_type}', "
                f"to_item='{self.to_item}', config_upload_at={self.config_upload_at})")


# BLOCK 5: Model Classes - AllocationByType
# ----------------------------------------------------------------------------
@dataclass
class AllocationByType:
    """
    Model class cho bảng AllocationByType
    Mapping các field từ BigQuery sang Python object
    """
    z_number: Optional[int] = None
    y_number: Optional[int] = None
    to_y_block_kr1: Optional[str] = None
    to_y_block_kr2: Optional[str] = None
    to_y_block_kr3: Optional[str] = None
    to_y_block_kr4: Optional[str] = None
    to_y_block_kr5: Optional[str] = None
    to_y_block_kr6: Optional[str] = None
    to_y_block_kr7: Optional[str] = None
    to_y_block_kr8: Optional[int] = None
    to_y_block_cdt1: Optional[int] = None
    to_y_block_cdt2: Optional[int] = None
    to_y_block_cdt3: Optional[int] = None
    to_y_block_cdt4: Optional[int] = None
    to_y_block_pt1: Optional[int] = None
    to_y_block_pt2: Optional[int] = None
    to_y_block_duration: Optional[int] = None
    to_y_block_pt1_prev: Optional[int] = None
    to_y_block_pt2_prev: Optional[int] = None
    to_y_block_duration_prev: Optional[int] = None
    to_y_block_own_type: Optional[int] = None
    to_y_block_ai_type: Optional[int] = None
    to_y_block_cty1: Optional[int] = None
    to_y_block_cty2: Optional[int] = None
    to_y_block_os_type: Optional[int] = None
    to_y_block_fu1: Optional[int] = None
    to_y_block_fu2: Optional[int] = None
    to_y_block_ch: Optional[int] = None
    to_y_block_egt1: Optional[int] = None
    to_y_block_egt2: Optional[int] = None
    to_y_block_egt3: Optional[int] = None
    to_y_block_egt4: Optional[int] = None
    to_y_block_hr1: Optional[int] = None
    to_y_block_hr2: Optional[int] = None
    to_y_block_hr3: Optional[int] = None
    to_y_block_sec: Optional[int] = None
    to_y_block_mx: Optional[int] = None
    to_y_block_dx: Optional[int] = None
    to_y_block_ppc: Optional[int] = None
    to_y_block_np: Optional[int] = None
    to_y_block_le1: Optional[int] = None
    to_y_block_le2: Optional[int] = None
    to_y_block_unit: Optional[int] = None
    by_block_by_type: Optional[str] = None
    config_upload_at: Optional[int] = None
    
    @classmethod
    def from_bigquery_row(cls, row) -> 'AllocationByType':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row
            
        return cls(
            z_number=data.get('ZNumber'),
            y_number=data.get('YNumber'),
            to_y_block_kr1=data.get('TO_Y_BLOCK_KR1'),
            to_y_block_kr2=data.get('TO_Y_BLOCK_KR2'),
            to_y_block_kr3=data.get('TO_Y_BLOCK_KR3'),
            to_y_block_kr4=data.get('TO_Y_BLOCK_KR4'),
            to_y_block_kr5=data.get('TO_Y_BLOCK_KR5'),
            to_y_block_kr6=data.get('TO_Y_BLOCK_KR6'),
            to_y_block_kr7=data.get('TO_Y_BLOCK_KR7'),
            to_y_block_kr8=data.get('TO_Y_BLOCK_KR8'),
            to_y_block_cdt1=data.get('TO_Y_BLOCK_CDT1'),
            to_y_block_cdt2=data.get('TO_Y_BLOCK_CDT2'),
            to_y_block_cdt3=data.get('TO_Y_BLOCK_CDT3'),
            to_y_block_cdt4=data.get('TO_Y_BLOCK_CDT4'),
            to_y_block_pt1=data.get('TO_Y_BLOCK_PT1'),
            to_y_block_pt2=data.get('TO_Y_BLOCK_PT2'),
            to_y_block_duration=data.get('TO_Y_BLOCK_Duration'),
            to_y_block_pt1_prev=data.get('TO_Y_BLOCK_PT1_PREV'),
            to_y_block_pt2_prev=data.get('TO_Y_BLOCK_PT2_PREV'),
            to_y_block_duration_prev=data.get('TO_Y_BLOCK_Duration_PREV'),
            to_y_block_own_type=data.get('TO_Y_BLOCK_OwnType'),
            to_y_block_ai_type=data.get('TO_Y_BLOCK_AIType'),
            to_y_block_cty1=data.get('TO_Y_BLOCK_CTY1'),
            to_y_block_cty2=data.get('TO_Y_BLOCK_CTY2'),
            to_y_block_os_type=data.get('TO_Y_BLOCK_OSType'),
            to_y_block_fu1=data.get('TO_Y_BLOCK_FU1'),
            to_y_block_fu2=data.get('TO_Y_BLOCK_FU2'),
            to_y_block_ch=data.get('TO_Y_BLOCK_CH'),
            to_y_block_egt1=data.get('TO_Y_BLOCK_EGT1'),
            to_y_block_egt2=data.get('TO_Y_BLOCK_EGT2'),
            to_y_block_egt3=data.get('TO_Y_BLOCK_EGT3'),
            to_y_block_egt4=data.get('TO_Y_BLOCK_EGT4'),
            to_y_block_hr1=data.get('TO_Y_BLOCK_HR1'),
            to_y_block_hr2=data.get('TO_Y_BLOCK_HR2'),
            to_y_block_hr3=data.get('TO_Y_BLOCK_HR3'),
            to_y_block_sec=data.get('TO_Y_BLOCK_SEC'),
            to_y_block_mx=data.get('TO_Y_BLOCK_MX'),
            to_y_block_dx=data.get('TO_Y_BLOCK_DX'),
            to_y_block_ppc=data.get('TO_Y_BLOCK_PPC'),
            to_y_block_np=data.get('TO_Y_BLOCK_NP'),
            to_y_block_le1=data.get('TO_Y_BLOCK_LE1'),
            to_y_block_le2=data.get('TO_Y_BLOCK_LE2'),
            to_y_block_unit=data.get('TO_Y_BLOCK_UNIT'),
            by_block_by_type=data.get('BY_BLOCK_ByType'),
            config_upload_at=data.get('Config_Upload_at')
        )
    
    @classmethod
    def from_dataframe(cls, df) -> List['AllocationByType']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [
            cls(
                z_number=row.get('ZNumber'),
                y_number=row.get('YNumber'),
                to_y_block_kr1=row.get('TO_Y_BLOCK_KR1'),
                to_y_block_kr2=row.get('TO_Y_BLOCK_KR2'),
                to_y_block_kr3=row.get('TO_Y_BLOCK_KR3'),
                to_y_block_kr4=row.get('TO_Y_BLOCK_KR4'),
                to_y_block_kr5=row.get('TO_Y_BLOCK_KR5'),
                to_y_block_kr6=row.get('TO_Y_BLOCK_KR6'),
                to_y_block_kr7=row.get('TO_Y_BLOCK_KR7'),
                to_y_block_kr8=row.get('TO_Y_BLOCK_KR8'),
                to_y_block_cdt1=row.get('TO_Y_BLOCK_CDT1'),
                to_y_block_cdt2=row.get('TO_Y_BLOCK_CDT2'),
                to_y_block_cdt3=row.get('TO_Y_BLOCK_CDT3'),
                to_y_block_cdt4=row.get('TO_Y_BLOCK_CDT4'),
                to_y_block_pt1=row.get('TO_Y_BLOCK_PT1'),
                to_y_block_pt2=row.get('TO_Y_BLOCK_PT2'),
                to_y_block_duration=row.get('TO_Y_BLOCK_Duration'),
                to_y_block_pt1_prev=row.get('TO_Y_BLOCK_PT1_PREV'),
                to_y_block_pt2_prev=row.get('TO_Y_BLOCK_PT2_PREV'),
                to_y_block_duration_prev=row.get('TO_Y_BLOCK_Duration_PREV'),
                to_y_block_own_type=row.get('TO_Y_BLOCK_OwnType'),
                to_y_block_ai_type=row.get('TO_Y_BLOCK_AIType'),
                to_y_block_cty1=row.get('TO_Y_BLOCK_CTY1'),
                to_y_block_cty2=row.get('TO_Y_BLOCK_CTY2'),
                to_y_block_os_type=row.get('TO_Y_BLOCK_OSType'),
                to_y_block_fu1=row.get('TO_Y_BLOCK_FU1'),
                to_y_block_fu2=row.get('TO_Y_BLOCK_FU2'),
                to_y_block_ch=row.get('TO_Y_BLOCK_CH'),
                to_y_block_egt1=row.get('TO_Y_BLOCK_EGT1'),
                to_y_block_egt2=row.get('TO_Y_BLOCK_EGT2'),
                to_y_block_egt3=row.get('TO_Y_BLOCK_EGT3'),
                to_y_block_egt4=row.get('TO_Y_BLOCK_EGT4'),
                to_y_block_hr1=row.get('TO_Y_BLOCK_HR1'),
                to_y_block_hr2=row.get('TO_Y_BLOCK_HR2'),
                to_y_block_hr3=row.get('TO_Y_BLOCK_HR3'),
                to_y_block_sec=row.get('TO_Y_BLOCK_SEC'),
                to_y_block_mx=row.get('TO_Y_BLOCK_MX'),
                to_y_block_dx=row.get('TO_Y_BLOCK_DX'),
                to_y_block_ppc=row.get('TO_Y_BLOCK_PPC'),
                to_y_block_np=row.get('TO_Y_BLOCK_NP'),
                to_y_block_le1=row.get('TO_Y_BLOCK_LE1'),
                to_y_block_le2=row.get('TO_Y_BLOCK_LE2'),
                to_y_block_unit=row.get('TO_Y_BLOCK_UNIT'),
                by_block_by_type=row.get('BY_BLOCK_ByType'),
                config_upload_at=row.get('Config_Upload_at')
            )
            for _, row in df.iterrows()
        ]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"AllocationByType(z_number={self.z_number}, y_number={self.y_number}, "
                f"kr1='{self.to_y_block_kr1}', kr2='{self.to_y_block_kr2}', kr3='{self.to_y_block_kr3}', "
                f"pt1={self.to_y_block_pt1}, pt2={self.to_y_block_pt2}, duration={self.to_y_block_duration}, "
                f"by_type='{self.by_block_by_type}', unit={self.to_y_block_unit})")


# BLOCK 6: Model Classes - SoCellRawFull
# ----------------------------------------------------------------------------
@dataclass
class SoCellRawFull:
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
    by_block_bytype: Optional[str] = None
    by_block_bypercent: Optional[float] = None
    
    @classmethod
    def from_bigquery_row(cls, row) -> 'SoCellRawFull':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row
            
        return cls(
            i_o=data.get('i_o'),
            z_block_zblock1_source=data.get('z_block_zblock1_source'),
            z_block_zblock1_pack=data.get('z_block_zblock1_pack'),
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
            by_block_bytype=data.get('by_block_bytype'),
            by_block_bypercent=data.get('by_block_bypercent')
        )
    
    @classmethod
    def from_dataframe(cls, df) -> List['SoCellRawFull']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"SoCellRawFull(fnf='{self.now_y_block_fnf_fnf}', "
                f"kr1='{self.now_y_block_kr_item_code_kr1}', "
                f"now_value={self.now_value}, by_type='{self.by_block_bytype}')")


# BLOCK 7: Khởi tạo BigQuery Client (cho Colab)
# ----------------------------------------------------------------------------
# Thay 'foxlearning' bằng project ID của bạn
PROJECT_ID = 'foxlearning'
client = bigquery.Client(project=PROJECT_ID)

print(f"✓ Đã kết nối tới BigQuery project: {PROJECT_ID}")


# BLOCK 8: Query AllocationALT
# ----------------------------------------------------------------------------
query_allocation_alt = """
SELECT
    ZNumber,
    FROM_ALT_FromALT,
    TO_ALT_ToALT,
    FROM_Y_BLOCK_FromType,
    TO_Y_BLOCK_ToType
FROM `foxlearning.allocation_config.AllocationALT_NativeTable` 
ORDER BY ZNumber ASC
"""

print("\n=== QUERY ALLOCATION ALT ===")
df_allocation_alt = client.query(query_allocation_alt).to_dataframe()
my_allocation_alt_items = AllocationALT.from_dataframe(df_allocation_alt)
print(f"✓ Tìm thấy {len(my_allocation_alt_items)} AllocationALT records")


# BLOCK 9: Query AllocationToItem và AllocationByType
# ----------------------------------------------------------------------------
# Lọc allocation với z_number = 422
for allocation in my_allocation_alt_items:
    if allocation.z_number != 422:
        continue
    
    print(f"\n{'='*70}")
    print(f"Processing: {allocation}")
    print(f"{'='*70}")
    
    # Query AllocationToItem
    query_to_item = f"""
    SELECT * 
    FROM `foxlearning.allocation_config.AllocationToItem_NativeTable` 
    WHERE TO_Y_BLOCK_ToType = "{allocation.to_type}"
    """
    
    df_to_item = client.query(query_to_item).to_dataframe()
    my_to_items = AllocationToItem.from_dataframe(df_to_item)
    print(f"\n→ Tìm thấy {len(my_to_items)} AllocationToItem records")
    
    # Query AllocationByType
    query_by_type = f"""
    SELECT * 
    FROM `foxlearning.allocation_config.AllocationByType_NativeTable` 
    WHERE ZNumber = {allocation.z_number}
    ORDER BY YNumber DESC
    """
    
    df_by_type = client.query(query_by_type).to_dataframe()
    my_allocation_by_type_items = AllocationByType.from_dataframe(df_by_type)
    print(f"→ Tìm thấy {len(my_allocation_by_type_items)} AllocationByType records")


# BLOCK 10: Query SoCellRawFull
# ----------------------------------------------------------------------------
# Lấy first AllocationByType item để demo
count_flag = 0
for my_allocation_by_type_item in my_allocation_by_type_items:
    if count_flag > 0:
        break
    
    print(f"\n→ Processing AllocationByType: {my_allocation_by_type_item}")
    
    # Query SoCellRawFull
    query_so_cell = """
    SELECT * 
    FROM `foxlearning.alloc_stage.so_cell_raw_full` 
    WHERE now_y_block_fnf_fnf = "KRF"
    AND now_y_block_kr_item_code_kr1 = "GI"
    """
    
    df_so_cell = client.query(query_so_cell).to_dataframe()
    from_so_cell_items = SoCellRawFull.from_dataframe(df_so_cell)
    
    print(f"\n→ Tìm thấy {len(from_so_cell_items)} SoCellRawFull records")
    print("\n→ Hiển thị 3 records đầu tiên:")
    for from_so_cell_item in from_so_cell_items[:3]:
        print(f"  {from_so_cell_item}")
    
    count_flag += 1

print("\n✓ Hoàn thành!")
