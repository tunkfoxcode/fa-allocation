import os
from dataclasses import dataclass
from typing import Optional, List
from google.cloud import bigquery
from google.oauth2 import service_account


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
        """
        Factory method để tạo instance từ BigQuery Row
        
        Args:
            row: BigQuery Row object hoặc dictionary
            
        Returns:
            AllocationALT instance
        """
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
        """
        Factory method để tạo list instances từ pandas DataFrame
        
        Args:
            df: pandas DataFrame từ BigQuery query result
            
        Returns:
            List of AllocationALT instances
        """
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
        """
        Factory method để tạo instance từ BigQuery Row
        
        Args:
            row: BigQuery Row object hoặc dictionary
            
        Returns:
            AllocationToItem instance
        """
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
        """
        Factory method để tạo list instances từ pandas DataFrame
        
        Args:
            df: pandas DataFrame từ BigQuery query result
            
        Returns:
            List of AllocationToItem instances
        """
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
        """
        Factory method để tạo instance từ BigQuery Row
        
        Args:
            row: BigQuery Row object hoặc dictionary
            
        Returns:
            AllocationByType instance
        """
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
        """
        Factory method để tạo list instances từ pandas DataFrame
        
        Args:
            df: pandas DataFrame từ BigQuery query result
            
        Returns:
            List of AllocationByType instances
        """
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


@dataclass
class AllocationByKR:
    """
    Model class cho bảng AllocationByKR_NativeTable
    Mapping các field từ BigQuery sang Python object
    """
    from_y_block_from_type: Optional[str] = None
    to_y_block_to_type: Optional[str] = None
    to_y_block_kr1: Optional[str] = None
    to_y_block_kr2: Optional[str] = None
    to_y_block_kr3: Optional[str] = None
    to_y_block_kr4: Optional[str] = None
    to_y_block_kr5: Optional[str] = None
    to_y_block_kr6: Optional[str] = None
    to_y_block_kr7: Optional[str] = None
    to_y_block_kr8: Optional[int] = None
    to_y_block_kr9: Optional[int] = None
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
    def from_bigquery_row(cls, row) -> 'AllocationByKR':
        """Factory method để tạo instance từ BigQuery Row"""
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row
            
        return cls(
            from_y_block_from_type=data.get('FROM_Y_BLOCK_FromType'),
            to_y_block_to_type=data.get('TO_Y_BLOCK_ToType'),
            to_y_block_kr1=data.get('TO_Y_BLOCK_KR1'),
            to_y_block_kr2=data.get('TO_Y_BLOCK_KR2'),
            to_y_block_kr3=data.get('TO_Y_BLOCK_KR3'),
            to_y_block_kr4=data.get('TO_Y_BLOCK_KR4'),
            to_y_block_kr5=data.get('TO_Y_BLOCK_KR5'),
            to_y_block_kr6=data.get('TO_Y_BLOCK_KR6'),
            to_y_block_kr7=data.get('TO_Y_BLOCK_KR7'),
            to_y_block_kr8=data.get('TO_Y_BLOCK_KR8'),
            to_y_block_kr9=data.get('TO_Y_BLOCK_KR9'),
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
    def from_dataframe(cls, df) -> List['AllocationByKR']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"AllocationByKR(from_type='{self.from_y_block_from_type}', to_type='{self.to_y_block_to_type}', "
                f"kr6='{self.to_y_block_kr6}', kr4='{self.to_y_block_kr4}', by_type='{self.by_block_by_type}')")


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
    def from_dataframe(cls, df) -> List['SoCell']:
        """Factory method để tạo list instances từ pandas DataFrame"""
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]
    
    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"SoCellRawFull(fnf='{self.now_y_block_fnf_fnf}', "
                f"kr1='{self.now_y_block_kr_item_code_kr1}', "
                f"now_value={self.now_value}, by_type='{self.by_block_bytype}')")


# YBLOCK Field Mapping: AllocationByType -> SoCell (NowYBlock)
YBLOCK_FIELD_MAPPING = {
    # KR fields
    'to_y_block_kr1': 'now_y_block_kr_item_code_kr1',
    'to_y_block_kr2': 'now_y_block_kr_item_code_kr2',
    'to_y_block_kr3': 'now_y_block_kr_item_code_kr3',
    'to_y_block_kr4': 'now_y_block_kr_item_code_kr4',
    'to_y_block_kr5': 'now_y_block_kr_item_code_kr5',
    'to_y_block_kr6': 'now_y_block_kr_item_code_kr6',
    'to_y_block_kr7': 'now_y_block_kr_item_code_kr7',
    'to_y_block_kr8': 'now_y_block_kr_item_code_kr8',
    # CDT fields
    'to_y_block_cdt1': 'now_y_block_cdt_cdt1',
    'to_y_block_cdt2': 'now_y_block_cdt_cdt2',
    'to_y_block_cdt3': 'now_y_block_cdt_cdt3',
    'to_y_block_cdt4': 'now_y_block_cdt_cdt4',
    # PT Now fields
    'to_y_block_pt1': 'now_y_block_ptnow_pt1',
    'to_y_block_pt2': 'now_y_block_ptnow_pt2',
    'to_y_block_duration': 'now_y_block_ptnow_duration',
    # PT Prev fields
    'to_y_block_pt1_prev': 'now_y_block_ptprev_pt1',
    'to_y_block_pt2_prev': 'now_y_block_ptprev_pt2',
    'to_y_block_duration_prev': 'now_y_block_ptprev_duration',
    # PT Fix fields
    'to_y_block_own_type': 'now_y_block_ptfix_owntype',
    'to_y_block_ai_type': 'now_y_block_ptfix_aitype',
    # PT Sub fields
    'to_y_block_cty1': 'now_y_block_ptsub_cty1',
    'to_y_block_cty2': 'now_y_block_ptsub_cty2',
    'to_y_block_os_type': 'now_y_block_ptsub_ostype',
    # Funnel fields
    'to_y_block_fu1': 'now_y_block_funnel_fu1',
    'to_y_block_fu2': 'now_y_block_funnel_fu2',
    # Channel field
    'to_y_block_ch': 'now_y_block_channel_ch',
    # Employee fields
    'to_y_block_egt1': 'now_y_block_employee_egt1',
    'to_y_block_egt2': 'now_y_block_employee_egt2',
    'to_y_block_egt3': 'now_y_block_employee_egt3',
    'to_y_block_egt4': 'now_y_block_employee_egt4',
    # HR fields
    'to_y_block_hr1': 'now_y_block_hr_hr1',
    'to_y_block_hr2': 'now_y_block_hr_hr2',
    'to_y_block_hr3': 'now_y_block_hr_hr3',
    # SEC field
    'to_y_block_sec': 'now_y_block_sec',
    # Period fields
    'to_y_block_mx': 'now_y_block_period_mx',
    'to_y_block_dx': 'now_y_block_period_dx',
    'to_y_block_ppc': 'now_y_block_period_ppc',
    'to_y_block_np': 'now_y_block_period_np',
    # LE fields
    'to_y_block_le1': 'now_y_block_le_le1',
    'to_y_block_le2': 'now_y_block_le_le2',
    # Unit field
    'to_y_block_unit': 'now_y_block_unit',
}


# YBLOCK Field Mapping: SoCell NowYBlock -> SoCell PrevYBlock
PREV_YBLOCK_FIELD_MAPPING = {
    # FNF field
    'now_y_block_fnf_fnf': 'prev_y_block_fnf_fnf',
    # KR fields
    'now_y_block_kr_item_code_kr1': 'prev_y_block_kr_item_code_kr1',
    'now_y_block_kr_item_code_kr2': 'prev_y_block_kr_item_code_kr2',
    'now_y_block_kr_item_code_kr3': 'prev_y_block_kr_item_code_kr3',
    'now_y_block_kr_item_code_kr4': 'prev_y_block_kr_item_code_kr4',
    'now_y_block_kr_item_code_kr5': 'prev_y_block_kr_item_code_kr5',
    'now_y_block_kr_item_code_kr6': 'prev_y_block_kr_item_code_kr6',
    'now_y_block_kr_item_code_kr7': 'prev_y_block_kr_item_code_kr7',
    'now_y_block_kr_item_code_kr8': 'prev_y_block_kr_item_code_kr8',
    'now_y_block_kr_item_name': 'prev_y_block_kr_item_name',
    # CDT fields
    'now_y_block_cdt_cdt1': 'prev_y_block_cdt_cdt1',
    'now_y_block_cdt_cdt2': 'prev_y_block_cdt_cdt2',
    'now_y_block_cdt_cdt3': 'prev_y_block_cdt_cdt3',
    'now_y_block_cdt_cdt4': 'prev_y_block_cdt_cdt4',
    # PT Now fields
    'now_y_block_ptnow_pt1': 'prev_y_block_ptnow_pt1',
    'now_y_block_ptnow_pt2': 'prev_y_block_ptnow_pt2',
    'now_y_block_ptnow_duration': 'prev_y_block_ptnow_duration',
    # PT Prev fields
    'now_y_block_ptprev_pt1': 'prev_y_block_ptprev_pt1',
    'now_y_block_ptprev_pt2': 'prev_y_block_ptprev_pt2',
    'now_y_block_ptprev_duration': 'prev_y_block_ptprev_duration',
    # PT Fix fields
    'now_y_block_ptfix_owntype': 'prev_y_block_ptfix_owntype',
    'now_y_block_ptfix_aitype': 'prev_y_block_ptfix_aitype',
    # PT Sub fields
    'now_y_block_ptsub_cty1': 'prev_y_block_ptsub_cty1',
    'now_y_block_ptsub_cty2': 'prev_y_block_ptsub_cty2',
    'now_y_block_ptsub_ostype': 'prev_y_block_ptsub_ostype',
    # Funnel fields
    'now_y_block_funnel_fu1': 'prev_y_block_funnel_fu1',
    'now_y_block_funnel_fu2': 'prev_y_block_funnel_fu2',
    # Channel field
    'now_y_block_channel_ch': 'prev_y_block_channel_ch',
    # Employee fields
    'now_y_block_employee_egt1': 'prev_y_block_employee_egt1',
    'now_y_block_employee_egt2': 'prev_y_block_employee_egt2',
    'now_y_block_employee_egt3': 'prev_y_block_employee_egt3',
    'now_y_block_employee_egt4': 'prev_y_block_employee_egt4',
    # HR fields
    'now_y_block_hr_hr1': 'prev_y_block_hr_hr1',
    'now_y_block_hr_hr2': 'prev_y_block_hr_hr2',
    'now_y_block_hr_hr3': 'prev_y_block_hr_hr3',
    # SEC field
    'now_y_block_sec': 'prev_y_block_sec',
    # Period fields
    'now_y_block_period_mx': 'prev_y_block_period_mx',
    'now_y_block_period_dx': 'prev_y_block_period_dx',
    'now_y_block_period_ppc': 'prev_y_block_period_ppc',
    'now_y_block_period_np': 'prev_y_block_period_np',
    # LE fields
    'now_y_block_le_le1': 'prev_y_block_le_le1',
    'now_y_block_le_le2': 'prev_y_block_le_le2',
    # Unit field
    'now_y_block_unit': 'prev_y_block_unit',
}


def build_so_cell_query(allocation_by_type_item: AllocationByType, project_id: str, dataset_id: str = 'alloc_stage', table_id: str = 'so_cell_raw_full') -> str:
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
    import pandas as pd
    
    where_conditions = []
    
    # Duyệt qua mapping và build WHERE conditions
    for by_type_field, so_cell_field in YBLOCK_FIELD_MAPPING.items():
        # Lấy giá trị từ AllocationByType instance
        value = getattr(allocation_by_type_item, by_type_field, None)
        
        # Skip nếu value là None, pandas NA, NaN, hoặc empty string
        if value is None:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, str) and value == '':
            continue
            
        # Format value dựa trên type
        if isinstance(value, str):
            # Escape single quotes trong string
            escaped_value = value.replace("'", "\\'")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"
        
        where_conditions.append(f"{so_cell_field} = {formatted_value}")
    
    # Build query
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"
    
    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)
    
    return query


def build_so_cell_prev_query(y_block_1: SoCell, x_period_1: str, z_number: int, project_id: str, dataset_id: str = 'alloc_stage', table_id: str = 'so_cell_raw_full') -> str:
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
    import pandas as pd
    
    where_conditions = []
    
    # Duyệt qua mapping và build WHERE conditions cho PrevYBlock
    for now_field, prev_field in PREV_YBLOCK_FIELD_MAPPING.items():
        # Lấy giá trị từ NowYBlock của y_block_1
        value = getattr(y_block_1, now_field, None)
        
        # Skip nếu value là None, pandas NA, NaN, hoặc empty string
        if value is None:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, str) and value == '':
            continue
            
        # Format value dựa trên type
        if isinstance(value, str):
            # Escape single quotes trong string
            escaped_value = value.replace("'", "\\'")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"
        
        # Add condition cho PrevYBlock field
        where_conditions.append(f"{prev_field} = {formatted_value}")
    
    # Add XPeriod condition (now_np)
    if x_period_1 is not None and not pd.isna(x_period_1):
        if isinstance(x_period_1, str):
            escaped_x_period = x_period_1.replace("'", "\\'")
            where_conditions.append(f"now_np = '{escaped_x_period}'")
        else:
            where_conditions.append(f"now_np = {x_period_1}")
    
    # Add ZNumber condition (from z_block)
    if z_number is not None:
        where_conditions.append(f"now_zblock2_alt = '{z_number}'")
    
    # Build query
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    query = f"SELECT * FROM `{table_name}`"
    
    if where_conditions:
        query += "\nWHERE " + "\nAND ".join(where_conditions)
    
    return query


class BigQueryConnector:
    def __init__(self, credentials_path=None, project_id=None):
        """
        Khởi tạo kết nối tới Google BigQuery
        
        Args:
            credentials_path: Đường dẫn tới file JSON service account key
            project_id: ID của Google Cloud Project
        """
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            self.client = bigquery.Client(project=project_id)
        
        print(f"✓ Đã kết nối thành công tới BigQuery project: {self.client.project}")
    
    def execute_query(self, query):
        """
        Thực thi query và trả về kết quả
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame chứa kết quả query
        """
        try:
            print(f"Đang thực thi query...")
            query_job = self.client.query(query)
            results = query_job.result()
            df = results.to_dataframe()
            print(f"✓ Query thành công! Trả về {len(df)} dòng dữ liệu")
            return df
        except Exception as e:
            print(f"✗ Lỗi khi thực thi query: {str(e)}")
            raise
    
    def list_datasets(self):
        """Liệt kê tất cả datasets trong project"""
        datasets = list(self.client.list_datasets())
        if datasets:
            print(f"Datasets trong project {self.client.project}:")
            for dataset in datasets:
                print(f"  - {dataset.dataset_id}")
        else:
            print(f"Project {self.client.project} không có dataset nào")
        return datasets
    
    def list_tables(self, dataset_id):
        """Liệt kê tất cả tables trong một dataset"""
        tables = list(self.client.list_tables(dataset_id))
        if tables:
            print(f"Tables trong dataset {dataset_id}:")
            for table in tables:
                print(f"  - {table.table_id}")
        else:
            print(f"Dataset {dataset_id} không có table nào")
        return tables
    
    def get_table_schema(self, dataset_id, table_id):
        """Lấy schema của một table"""
        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)
        print(f"Schema của table {table_ref}:")
        for field in table.schema:
            print(f"  - {field.name}: {field.field_type}")
        return table.schema


def main():
    # Configuration
    credentials_path = "/home/tunk/Desktop/foxlearning-6ddb4fb2192a.json"
    project_id = "foxlearning"
    
    # Dataset names
    allocation_config_dataset_name = "allocation_config"
    alloc_data_dataset_name = "alloc_stage"
    
    # Table names
    allocation_alt_table_name = "AllocationALT_NativeTable"
    allocation_to_item_table_name = "AllocationToItem_NativeTable"
    allocation_by_type_table_name = "AllocationByType_NativeTable"
    allocation_by_kr_table_name = "AllocationByKR_NativeTable"
    so_cell_table_name = "so_cell_raw_full"
    
    try:
        # Khởi tạo connector
        bq = BigQueryConnector(
            credentials_path=credentials_path,
            project_id=project_id
        )
        
        #Step20 Query from AllocationALT: MyAllocationALTItem (N)
        print("\n=== QUERY ALLOCATION ALT ===")
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

        #Step30 Foreach MyAllocationALTItem (ZNumber, MyFromALT, MyToALT, MyFromType, MyToType) (ZNumber INCREASING)
        for allocation in my_allocation_alt_items:
            # TODO remove it to calculate all
            if allocation.z_number != 422:
                continue
            print(f"  {allocation}")
            #Step35 Query from AllocationToItem: (MyFromType) -> MyFromItemAllowed (N) // PT0 -> "Null"; PT1 -> Game, Util, Productivity...



            #Step40 Query from AllocationToItem: (MyToType) -> MyToItem (N)
            # Query AllocationToItem dựa trên to_type của allocation
            query_to_item = f"""
            SELECT * 
            FROM `{project_id}.{allocation_config_dataset_name}.{allocation_to_item_table_name}` 
            WHERE TO_Y_BLOCK_ToType = "{allocation.to_type}"
            """
            
            my_to_items_raw = bq.execute_query(query_to_item)
            
            # Chuyển đổi sang list of AllocationToItem objects
            my_to_items = AllocationToItem.from_dataframe(my_to_items_raw)
            
            print(f"\n  → Tìm thấy {len(my_to_items)} items cho to_type='{allocation.to_type}'")

            #Step50 Query from AllocationByType: (ZNumber) -> MyAllocationByTypeItem (YNumber, Y-Block, MyByType) (N)
            # Query AllocationByType dựa trên z_number của allocation
            query_by_type = f"""
            SELECT * 
            FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_type_table_name}` 
            WHERE ZNumber = {allocation.z_number}
            ORDER BY YNumber DESC
            """
            
            my_by_type_raw = bq.execute_query(query_by_type)
            
            # Chuyển đổi sang list of AllocationByType objects
            my_allocation_by_type_items = AllocationByType.from_dataframe(my_by_type_raw)
            
            print(f"  → Tìm thấy {len(my_allocation_by_type_items)} records trong AllocationByType cho z_number={allocation.z_number}")

            #TODO remove it to calculate all
            count_flag = 0

            #Step60 Foreach MyAllocationByTypeItem (YNumber DECREASING):
            for my_allocation_by_type_item in my_allocation_by_type_items:
                if my_allocation_by_type_item.to_y_block_kr1 != 'GI':
                    continue
                if count_flag > 0:
                    continue
                print(f"    {my_allocation_by_type_item}")
                
                #Step70 Query from SOCell: (MyAllocationByTypeItem.Y-Block, XPeriod, Z-Block) -> FromSOCellItem (N)
                #Mapping each yblock from by_type to so_cell
                print(f"\n  → Building dynamic query for SoCell...")
                
                # Build query động dựa trên Y-block fields của AllocationByType
                query_so_cell = build_so_cell_query(
                    my_allocation_by_type_item, 
                    project_id, 
                    dataset_id=alloc_data_dataset_name, 
                    table_id=so_cell_table_name
                )
                
                print(f"  → Generated query:\n{query_so_cell}\n")
                
                my_so_cell_raw = bq.execute_query(query_so_cell)

                # Chuyển đổi sang list of SoCellRawFull objects
                from_so_cell_items = SoCell.from_dataframe(my_so_cell_raw)

                #Step80 Foreach FromSOCelItem(N)
                for from_so_cell_item in from_so_cell_items[0:3]:
                    print(f"    {from_so_cell_item}")
                    # Step90 (YBlock1, XPeriod1, Value1) = FromSOCelItem(NowYBlock, XPeriod, NowValue)
                    y_block_1 = from_so_cell_item
                    x_period_1 = from_so_cell_item.now_np
                    value_1 = from_so_cell_item.now_value


                    # Step100 MyFromItem = GetItem (YBlock1, MyFromType)
                    # Tam thoi skip Step100
                    
                    #Step110 Query from SOCell: 
                    # (SOCellItem.PrevYBlock = YBlock1 
                    # AND SOCellItem.XPeriod = XPeriod1 
                    # AND SOCellItem.ZNumber = MyAllocationALTItem.ZNumber) (N)
                    print(f"\nStep110    → Building query for SoCell with PrevYBlock matching...")
                    
                    # Build query động cho PrevYBlock
                    query_so_cell_prev = build_so_cell_prev_query(
                        y_block_1=y_block_1,
                        x_period_1=x_period_1,
                        z_number=allocation.z_number,
                        project_id=project_id,
                        dataset_id=alloc_data_dataset_name,
                        table_id=so_cell_table_name
                    )
                    
                    print(f"Step110    → Generated PrevYBlock query:\n{query_so_cell_prev}\n")
                    
                    # Execute query
                    so_cell_raw = bq.execute_query(query_so_cell_prev)
                    so_cells = SoCell.from_dataframe(so_cell_raw)
                    print(f"Step110   → Tìm thấy {len(so_cells)} ToSoCell records")

                    #Step120 If (N = 0)
                    if len(so_cells) > 0:
                        continue
                    
                    #Step120 Start allocating
                    # Query AllocationByKR: WHERE TO_Y_BLOCK_KR6 = MyFromType 
                    # AND TO_Y_BLOCK_KR4 = MyToType 
                    # AND BY_BLOCK_ByType = MyByType
                    print(f"\nStep130   → Querying AllocationByKR...")
                    
                    my_from_type = allocation.from_type  # MyFromType
                    my_to_type = allocation.to_type      # MyToType
                    my_by_type = my_allocation_by_type_item.by_block_by_type  # MyByType
                    
                    query_allocation_by_kr = f"""
                    SELECT * 
                    FROM `{project_id}.{allocation_config_dataset_name}.{allocation_by_kr_table_name}` 
                    WHERE TO_Y_BLOCK_KR6 = '{my_from_type}'
                    AND TO_Y_BLOCK_KR4 = '{my_to_type}'
                    AND BY_BLOCK_ByType = '{my_by_type}'
                    """
                    
                    print(f"Step130   → Query: FROM_TYPE={my_from_type}, TO_TYPE={my_to_type}, BY_TYPE={my_by_type}")
                    
                    allocation_by_kr_raw = bq.execute_query(query_allocation_by_kr)
                    allocation_by_kr_items = AllocationByKR.from_dataframe(allocation_by_kr_raw)
                    
                    print(f"Step130   → Tìm thấy {len(allocation_by_kr_items)} AllocationByKR records")

                    if len(allocation_by_kr_items) < 0:
                        continue
                    allocation_by_kr_item = allocation_by_kr_items[0]
                    #Step130
                    # KRBlock3 = BY_BLOCK_ByType & "-TO-" & TO_Y_BLOCK_KR4 & "-FROM-" & TO_Y_BLOCK_KR6
                    # kr_block_3 = allocation_by_kr_item.by_block_by_type + "-TO-" + allocation_by_kr_item.to_y_block_kr4 + "-FROM-" + allocation_by_kr_item.to_y_block_kr6

                    kr_block_3 = allocation_by_kr_item

                    
                    #Step140 Foreach MyToItem (N)
                    for my_to_item in my_to_items:
                        filter_block_3 = my_to_item
                        y_block_3 = {
                            "kr_block_3": kr_block_3,
                            "filter_block_3": filter_block_3
                        }

                count_flag = count_flag + 1
            
        
    except Exception as e:
        print(f"Lỗi: {str(e)}")


if __name__ == '__main__':
    main()
