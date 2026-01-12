from dataclasses import dataclass
from typing import Optional, List


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
