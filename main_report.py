from calculate.report_runner import run_report

if __name__ == '__main__':
    my_rep_temp = "KRF-L4.CDT1"
    my_z_block_plan =  "my_z_block_plan"
    my_z_block_forecast = "my_z_block_forecast"
    my_alt =  "my_alt"
    my_last_report_month = "M2512"
    my_last_actual_month =  "M2510"

    run_report(
        my_rep_temp=my_rep_temp,
        my_z_block_plan=my_z_block_plan,
        my_z_block_forecast=my_z_block_forecast,
        my_alt=my_alt,
        my_last_report_month=my_last_report_month,
        my_last_actual_month=my_last_actual_month,
    )

