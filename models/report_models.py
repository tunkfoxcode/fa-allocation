from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime


@dataclass
class RepPage:
    """
    Model class cho bảng RepPage
    Mapping các field từ BigQuery sang Python object
    """
    y_number1: Optional[int] = None
    my_rep_temp: Optional[str] = None
    z_block_zblock_plan_source: Optional[str] = None
    z_block_zblock_plan_pack: Optional[str] = None
    z_block_zblock_plan_scenario: Optional[str] = None
    z_block_zblock_plan_run: Optional[str] = None


    z_block_forecast_source: Optional[str] = None
    z_block_forecast_pack: Optional[str] = None
    z_block_forecast_scenario: Optional[str] = None
    z_block_forecast_run: Optional[str] = None


    now_zblock2_alt: Optional[str] = None

    time_x_block_last_report_month: Optional[str] = None
    time_x_block_last_acttual_month: Optional[str] = None
    time_x_block_upload_at: Optional[datetime] = None

    @classmethod
    def from_bigquery_row(cls, row) -> 'RepPage':
        """
        Factory method để tạo instance từ BigQuery Row

        Args:
            row: BigQuery Row object hoặc dictionary

        Returns:
            RepPage instance
        """
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row

        return cls(
            y_number1=data.get('YNumber1'),
            my_rep_temp=data.get('MyRepTemp'),
            z_block_zblock_plan_source=data.get('Z_BLOCK_ZBlockPlan_Source'),
            z_block_zblock_plan_pack=data.get('Z_BLOCK_ZBlockPlan_Pack'),
            z_block_zblock_plan_scenario=data.get('Z_BLOCK_ZBlockPlan_Scenario'),
            z_block_zblock_plan_run=data.get('Z_BLOCK_ZBlockPlan_Run'),
            z_block_forecast_source=data.get('ZBlockForecast_Source'),
            z_block_forecast_pack=data.get('ZBlockForecast_Pack'),
            z_block_forecast_scenario=data.get('ZBlockForecast_Scenario'),
            z_block_forecast_run=data.get('ZBlockForecast_Run'),
            now_zblock2_alt=data.get('NOW_ZBlock2_ALT'),
            time_x_block_last_report_month=data.get('TIME_X_BLOCK_LastReportMonth'),
            time_x_block_last_acttual_month=data.get('TIME_X_BLOCK_LastActtualMonth'),
            time_x_block_upload_at=data.get('TIME_X_BLOCK_Upload_at')
        )

    @classmethod
    def from_dataframe(cls, df) -> List['RepPage']:
        """
        Factory method để tạo list instances từ pandas DataFrame

        Args:
            df: pandas DataFrame từ BigQuery query result

        Returns:
            List of RepPage instances
        """
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]

    def to_bigquery_dict(self) -> dict:
        """
        Convert instance to dictionary with BigQuery field names
        
        Returns:
            Dictionary with BigQuery-compatible field names
        """
        return {
            'YNumber1': self.y_number1,
            'MyRepTemp': self.my_rep_temp,
            'Z_BLOCK_ZBlockPlan_Source': self.z_block_zblock_plan_source,
            'Z_BLOCK_ZBlockPlan_Pack': self.z_block_zblock_plan_pack,
            'Z_BLOCK_ZBlockPlan_Scenario': self.z_block_zblock_plan_scenario,
            'Z_BLOCK_ZBlockPlan_Run': self.z_block_zblock_plan_run,
            'ZBlockForecast_Source': self.z_block_forecast_source,
            'ZBlockForecast_Pack': self.z_block_forecast_pack,
            'ZBlockForecast_Scenario': self.z_block_forecast_scenario,
            'ZBlockForecast_Run': self.z_block_forecast_run,
            'NOW_ZBlock2_ALT': self.now_zblock2_alt,
            'TIME_X_BLOCK_LastReportMonth': self.time_x_block_last_report_month,
            'TIME_X_BLOCK_LastActtualMonth': self.time_x_block_last_acttual_month,
            'TIME_X_BLOCK_Upload_at': self.time_x_block_upload_at
        }

    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"RepPage(y_number1={self.y_number1}, "
                f"my_rep_temp='{self.my_rep_temp}', "
                f"plan_source='{self.z_block_zblock_plan_source}', "
                f"forecast_source='{self.z_block_forecast_source}', "
                f"now_alt='{self.now_zblock2_alt}')")


@dataclass
class RepTemp:
    """
    Model class cho bảng RepTemp
    Mapping các field từ BigQuery sang Python object
    """
    z_number: Optional[int] = None
    rep_temp_type: Optional[str] = None

    @classmethod
    def from_bigquery_row(cls, row) -> 'RepTemp':
        """
        Factory method để tạo instance từ BigQuery Row

        Args:
            row: BigQuery Row object hoặc dictionary

        Returns:
            RepTemp instance
        """
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row

        return cls(
            z_number=data.get('ZNumber'),
            rep_temp_type=data.get('REP_TEMP_TYPE')
        )

    @classmethod
    def from_dataframe(cls, df) -> List['RepTemp']:
        """
        Factory method để tạo list instances từ pandas DataFrame

        Args:
            df: pandas DataFrame từ BigQuery query result

        Returns:
            List of RepTemp instances
        """
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]

    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"RepTemp(z_number={self.z_number}, "
                f"rep_temp_type='{self.rep_temp_type}')")


@dataclass
class RepTempBlock:
    """
    Model class cho bảng RepTempBlock
    Mapping các field từ BigQuery sang Python object
    """
    y_number2: Optional[int] = None
    my_rep_temp: Optional[str] = None
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
    now_y_block_td_bu: Optional[str] = None
    now_np: Optional[str] = None

    @classmethod
    def from_bigquery_row(cls, row) -> 'RepTempBlock':
        """
        Factory method để tạo instance từ BigQuery Row

        Args:
            row: BigQuery Row object hoặc dictionary

        Returns:
            RepTempBlock instance
        """
        if hasattr(row, 'items'):
            data = dict(row.items())
        else:
            data = row

        return cls(
            y_number2=data.get('YNumber2'),
            my_rep_temp=data.get('MyRepTemp'),
            now_y_block_fnf_fnf=data.get('NOW_Y_BLOCK_FNF_FNF'),
            now_y_block_kr_item_code_kr1=data.get('NOW_Y_BLOCK_KR_Item_Code_KR1'),
            now_y_block_kr_item_code_kr2=data.get('NOW_Y_BLOCK_KR_Item_Code_KR2'),
            now_y_block_kr_item_code_kr3=data.get('NOW_Y_BLOCK_KR_Item_Code_KR3'),
            now_y_block_kr_item_code_kr4=data.get('NOW_Y_BLOCK_KR_Item_Code_KR4'),
            now_y_block_kr_item_code_kr5=data.get('NOW_Y_BLOCK_KR_Item_Code_KR5'),
            now_y_block_kr_item_code_kr6=data.get('NOW_Y_BLOCK_KR_Item_Code_KR6'),
            now_y_block_kr_item_code_kr7=data.get('NOW_Y_BLOCK_KR_Item_Code_KR7'),
            now_y_block_kr_item_code_kr8=data.get('NOW_Y_BLOCK_KR_Item_Code_KR8'),
            now_y_block_kr_item_name=data.get('NOW_Y_BLOCK_KR_Item_Name'),
            now_y_block_cdt_cdt1=data.get('NOW_Y_BLOCK_CDT_CDT1'),
            now_y_block_cdt_cdt2=data.get('NOW_Y_BLOCK_CDT_CDT2'),
            now_y_block_cdt_cdt3=data.get('NOW_Y_BLOCK_CDT_CDT3'),
            now_y_block_cdt_cdt4=data.get('NOW_Y_BLOCK_CDT_CDT4'),
            now_y_block_ptnow_pt1=data.get('NOW_Y_BLOCK_PTNow_PT1'),
            now_y_block_ptnow_pt2=data.get('NOW_Y_BLOCK_PTNow_PT2'),
            now_y_block_ptnow_duration=data.get('NOW_Y_BLOCK_PTNow_Duration'),
            now_y_block_ptprev_pt1=data.get('NOW_Y_BLOCK_PTPrev_PT1'),
            now_y_block_ptprev_pt2=data.get('NOW_Y_BLOCK_PTPrev_PT2'),
            now_y_block_ptprev_duration=data.get('NOW_Y_BLOCK_PTPrev_Duration'),
            now_y_block_ptfix_owntype=data.get('NOW_Y_BLOCK_PTFix_OwnType'),
            now_y_block_ptfix_aitype=data.get('NOW_Y_BLOCK_PTFix_AIType'),
            now_y_block_ptsub_cty1=data.get('NOW_Y_BLOCK_PTSub_CTY1'),
            now_y_block_ptsub_cty2=data.get('NOW_Y_BLOCK_PTSub_CTY2'),
            now_y_block_ptsub_ostype=data.get('NOW_Y_BLOCK_PTSub_OSType'),
            now_y_block_funnel_fu1=data.get('NOW_Y_BLOCK_Funnel_FU1'),
            now_y_block_funnel_fu2=data.get('NOW_Y_BLOCK_Funnel_FU2'),
            now_y_block_channel_ch=data.get('NOW_Y_BLOCK_Channel_CH'),
            now_y_block_employee_egt1=data.get('NOW_Y_BLOCK_Employee_EGT1'),
            now_y_block_employee_egt2=data.get('NOW_Y_BLOCK_Employee_EGT2'),
            now_y_block_employee_egt3=data.get('NOW_Y_BLOCK_Employee_EGT3'),
            now_y_block_employee_egt4=data.get('NOW_Y_BLOCK_Employee_EGT4'),
            now_y_block_hr_hr1=data.get('NOW_Y_BLOCK_HR_HR1'),
            now_y_block_hr_hr2=data.get('NOW_Y_BLOCK_HR_HR2'),
            now_y_block_hr_hr3=data.get('NOW_Y_BLOCK_HR_HR3'),
            now_y_block_sec=data.get('NOW_Y_BLOCK_SEC'),
            now_y_block_period_mx=data.get('NOW_Y_BLOCK_Period_MX'),
            now_y_block_period_dx=data.get('NOW_Y_BLOCK_Period_DX'),
            now_y_block_period_ppc=data.get('NOW_Y_BLOCK_Period_PPC'),
            now_y_block_period_np=data.get('NOW_Y_BLOCK_Period_NP'),
            now_y_block_le_le1=data.get('NOW_Y_BLOCK_LE_LE1'),
            now_y_block_le_le2=data.get('NOW_Y_BLOCK_LE_LE2'),
            now_y_block_unit=data.get('NOW_Y_BLOCK_UNIT'),
            now_y_block_td_bu=data.get('NOW_Y_BLOCK_TD_BU'),
            now_np=data.get('NOW_NP')
        )

    @classmethod
    def from_dataframe(cls, df) -> List['RepTempBlock']:
        """
        Factory method để tạo list instances từ pandas DataFrame

        Args:
            df: pandas DataFrame từ BigQuery query result

        Returns:
            List of RepTempBlock instances
        """
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]

    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"RepTempBlock(y_number2={self.y_number2}, "
                f"my_rep_temp='{self.my_rep_temp}', "
                f"fnf='{self.now_y_block_fnf_fnf}', "
                f"kr1='{self.now_y_block_kr_item_code_kr1}')")


@dataclass
class RepCell:
    """
    Model class cho bảng RepCell
    Lưu trữ dữ liệu report cell với các filter và KR fields
    """
    z_number: Optional[int] = None
    y_number1: Optional[int] = None
    y_number2: Optional[int] = None
    y_number3: Optional[int] = None
    my_rep_page: Optional[str] = None
    my_rep_temp_block: Optional[str] = None
    z_block_type: Optional[str] = None
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
    now_y_block_td_bu: Optional[str] = None
    now_np: Optional[str] = None
    now_value: Optional[float] = None

    @classmethod
    def from_bigquery_row(cls, row) -> 'RepCell':
        """
        Tạo RepCell instance từ BigQuery row
        """
        return cls(
            z_number=row.get('ZNumber'),
            y_number1=row.get('YNumber1'),
            y_number2=row.get('YNumber2'),
            y_number3=row.get('YNumber3'),
            my_rep_page=row.get('MyRepPage'),
            my_rep_temp_block=row.get('MyRepTempBlock'),
            z_block_type=row.get('Z_BLOCK_TYPE'),
            now_y_block_kr_item_code_kr1=row.get('NOW_Y_BLOCK_KR_Item_Code_KR1'),
            now_y_block_kr_item_code_kr2=row.get('NOW_Y_BLOCK_KR_Item_Code_KR2'),
            now_y_block_kr_item_code_kr3=row.get('NOW_Y_BLOCK_KR_Item_Code_KR3'),
            now_y_block_kr_item_code_kr4=row.get('NOW_Y_BLOCK_KR_Item_Code_KR4'),
            now_y_block_kr_item_code_kr5=row.get('NOW_Y_BLOCK_KR_Item_Code_KR5'),
            now_y_block_kr_item_code_kr6=row.get('NOW_Y_BLOCK_KR_Item_Code_KR6'),
            now_y_block_kr_item_code_kr7=row.get('NOW_Y_BLOCK_KR_Item_Code_KR7'),
            now_y_block_kr_item_code_kr8=row.get('NOW_Y_BLOCK_KR_Item_Code_KR8'),
            now_y_block_kr_item_name=row.get('NOW_Y_BLOCK_KR_Item_Name'),
            now_y_block_cdt_cdt1=row.get('NOW_Y_BLOCK_CDT_CDT1'),
            now_y_block_cdt_cdt2=row.get('NOW_Y_BLOCK_CDT_CDT2'),
            now_y_block_cdt_cdt3=row.get('NOW_Y_BLOCK_CDT_CDT3'),
            now_y_block_cdt_cdt4=row.get('NOW_Y_BLOCK_CDT_CDT4'),
            now_y_block_ptnow_pt1=row.get('NOW_Y_BLOCK_PTNow_PT1'),
            now_y_block_ptnow_pt2=row.get('NOW_Y_BLOCK_PTNow_PT2'),
            now_y_block_ptnow_duration=row.get('NOW_Y_BLOCK_PTNow_Duration'),
            now_y_block_ptprev_pt1=row.get('NOW_Y_BLOCK_PTPrev_PT1'),
            now_y_block_ptprev_pt2=row.get('NOW_Y_BLOCK_PTPrev_PT2'),
            now_y_block_ptprev_duration=row.get('NOW_Y_BLOCK_PTPrev_Duration'),
            now_y_block_ptfix_owntype=row.get('NOW_Y_BLOCK_PTFix_OwnType'),
            now_y_block_ptfix_aitype=row.get('NOW_Y_BLOCK_PTFix_AIType'),
            now_y_block_ptsub_cty1=row.get('NOW_Y_BLOCK_PTSub_CTY1'),
            now_y_block_ptsub_cty2=row.get('NOW_Y_BLOCK_PTSub_CTY2'),
            now_y_block_ptsub_ostype=row.get('NOW_Y_BLOCK_PTSub_OSType'),
            now_y_block_funnel_fu1=row.get('NOW_Y_BLOCK_Funnel_FU1'),
            now_y_block_funnel_fu2=row.get('NOW_Y_BLOCK_Funnel_FU2'),
            now_y_block_channel_ch=row.get('NOW_Y_BLOCK_Channel_CH'),
            now_y_block_employee_egt1=row.get('NOW_Y_BLOCK_Employee_EGT1'),
            now_y_block_employee_egt2=row.get('NOW_Y_BLOCK_Employee_EGT2'),
            now_y_block_employee_egt3=row.get('NOW_Y_BLOCK_Employee_EGT3'),
            now_y_block_employee_egt4=row.get('NOW_Y_BLOCK_Employee_EGT4'),
            now_y_block_hr_hr1=row.get('NOW_Y_BLOCK_HR_HR1'),
            now_y_block_hr_hr2=row.get('NOW_Y_BLOCK_HR_HR2'),
            now_y_block_hr_hr3=row.get('NOW_Y_BLOCK_HR_HR3'),
            now_y_block_sec=row.get('NOW_Y_BLOCK_SEC'),
            now_y_block_period_mx=row.get('NOW_Y_BLOCK_Period_MX'),
            now_y_block_period_dx=row.get('NOW_Y_BLOCK_Period_DX'),
            now_y_block_period_ppc=row.get('NOW_Y_BLOCK_Period_PPC'),
            now_y_block_period_np=row.get('NOW_Y_BLOCK_Period_NP'),
            now_y_block_le_le1=row.get('NOW_Y_BLOCK_LE_LE1'),
            now_y_block_le_le2=row.get('NOW_Y_BLOCK_LE_LE2'),
            now_y_block_unit=row.get('NOW_Y_BLOCK_UNIT'),
            now_y_block_td_bu=row.get('NOW_Y_BLOCK_TD_BU'),
            now_np=row.get('NOW_NP'),
            now_value=row.get('NOW_VALUE')
        )

    @classmethod
    def from_dataframe(cls, df) -> List['RepCell']:
        """
        Tạo list RepCell instances từ pandas DataFrame
        """
        return [cls.from_bigquery_row(row) for _, row in df.iterrows()]

    def to_bigquery_dict(self) -> dict:
        """
        Convert RepCell instance to dictionary with BigQuery field names
        """
        return {
            'ZNumber': self.z_number,
            'YNumber1': self.y_number1,
            'YNumber2': self.y_number2,
            'YNumber3': self.y_number3,
            'MyRepPage': self.my_rep_page,
            'MyRepTempBlock': self.my_rep_temp_block,
            'Z_BLOCK_TYPE': self.z_block_type,
            'NOW_Y_BLOCK_KR_Item_Code_KR1': self.now_y_block_kr_item_code_kr1,
            'NOW_Y_BLOCK_KR_Item_Code_KR2': self.now_y_block_kr_item_code_kr2,
            'NOW_Y_BLOCK_KR_Item_Code_KR3': self.now_y_block_kr_item_code_kr3,
            'NOW_Y_BLOCK_KR_Item_Code_KR4': self.now_y_block_kr_item_code_kr4,
            'NOW_Y_BLOCK_KR_Item_Code_KR5': self.now_y_block_kr_item_code_kr5,
            'NOW_Y_BLOCK_KR_Item_Code_KR6': self.now_y_block_kr_item_code_kr6,
            'NOW_Y_BLOCK_KR_Item_Code_KR7': self.now_y_block_kr_item_code_kr7,
            'NOW_Y_BLOCK_KR_Item_Code_KR8': self.now_y_block_kr_item_code_kr8,
            'NOW_Y_BLOCK_KR_Item_Name': self.now_y_block_kr_item_name,
            'NOW_Y_BLOCK_CDT_CDT1': self.now_y_block_cdt_cdt1,
            'NOW_Y_BLOCK_CDT_CDT2': self.now_y_block_cdt_cdt2,
            'NOW_Y_BLOCK_CDT_CDT3': self.now_y_block_cdt_cdt3,
            'NOW_Y_BLOCK_CDT_CDT4': self.now_y_block_cdt_cdt4,
            'NOW_Y_BLOCK_PTNow_PT1': self.now_y_block_ptnow_pt1,
            'NOW_Y_BLOCK_PTNow_PT2': self.now_y_block_ptnow_pt2,
            'NOW_Y_BLOCK_PTNow_Duration': self.now_y_block_ptnow_duration,
            'NOW_Y_BLOCK_PTPrev_PT1': self.now_y_block_ptprev_pt1,
            'NOW_Y_BLOCK_PTPrev_PT2': self.now_y_block_ptprev_pt2,
            'NOW_Y_BLOCK_PTPrev_Duration': self.now_y_block_ptprev_duration,
            'NOW_Y_BLOCK_PTFix_OwnType': self.now_y_block_ptfix_owntype,
            'NOW_Y_BLOCK_PTFix_AIType': self.now_y_block_ptfix_aitype,
            'NOW_Y_BLOCK_PTSub_CTY1': self.now_y_block_ptsub_cty1,
            'NOW_Y_BLOCK_PTSub_CTY2': self.now_y_block_ptsub_cty2,
            'NOW_Y_BLOCK_PTSub_OSType': self.now_y_block_ptsub_ostype,
            'NOW_Y_BLOCK_Funnel_FU1': self.now_y_block_funnel_fu1,
            'NOW_Y_BLOCK_Funnel_FU2': self.now_y_block_funnel_fu2,
            'NOW_Y_BLOCK_Channel_CH': self.now_y_block_channel_ch,
            'NOW_Y_BLOCK_Employee_EGT1': self.now_y_block_employee_egt1,
            'NOW_Y_BLOCK_Employee_EGT2': self.now_y_block_employee_egt2,
            'NOW_Y_BLOCK_Employee_EGT3': self.now_y_block_employee_egt3,
            'NOW_Y_BLOCK_Employee_EGT4': self.now_y_block_employee_egt4,
            'NOW_Y_BLOCK_HR_HR1': self.now_y_block_hr_hr1,
            'NOW_Y_BLOCK_HR_HR2': self.now_y_block_hr_hr2,
            'NOW_Y_BLOCK_HR_HR3': self.now_y_block_hr_hr3,
            'NOW_Y_BLOCK_SEC': self.now_y_block_sec,
            'NOW_Y_BLOCK_Period_MX': self.now_y_block_period_mx,
            'NOW_Y_BLOCK_Period_DX': self.now_y_block_period_dx,
            'NOW_Y_BLOCK_Period_PPC': self.now_y_block_period_ppc,
            'NOW_Y_BLOCK_Period_NP': self.now_y_block_period_np,
            'NOW_Y_BLOCK_LE_LE1': self.now_y_block_le_le1,
            'NOW_Y_BLOCK_LE_LE2': self.now_y_block_le_le2,
            'NOW_Y_BLOCK_UNIT': self.now_y_block_unit,
            'NOW_Y_BLOCK_TD_BU': self.now_y_block_td_bu,
            'NOW_NP': self.now_np,
            'NOW_VALUE': self.now_value
        }

    def __repr__(self) -> str:
        """String representation cho debugging"""
        return (f"RepCell(z_number={self.z_number}, "
                f"y_number1={self.y_number1}, "
                f"y_number2={self.y_number2}, "
                f"y_number3={self.y_number3}, "
                f"z_block_type='{self.z_block_type}', "
                f"now_value={self.now_value})")
