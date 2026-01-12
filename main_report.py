from calculate.report_runner import run_report

if __name__ == '__main__':
    my_rep_temp = "KRF-L4.CDT0"
    my_z_block_plan =  "PLAN-CA-Future-CAC"
    my_z_block_forecast = "FORECAST-CA-Future-CAC"
    my_alt =  "PLA4"
    my_last_report_month = "M2603"
    my_last_actual_month =  "M2512"

    run_report(
        my_rep_temp=my_rep_temp,
        my_z_block_plan=my_z_block_plan,
        my_z_block_forecast=my_z_block_forecast,
        my_alt=my_alt,
        my_last_report_month=my_last_report_month,
        my_last_actual_month=my_last_actual_month,
    )