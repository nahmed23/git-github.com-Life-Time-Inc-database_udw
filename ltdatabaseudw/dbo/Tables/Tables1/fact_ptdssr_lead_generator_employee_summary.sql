CREATE TABLE [dbo].[fact_ptdssr_lead_generator_employee_summary] (
    [fact_ptdssr_lead_generator_employee_summary_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [avg_revenue_connection]                         DECIMAL (26, 6) NULL,
    [avg_revenue_sale]                               DECIMAL (26, 6) NULL,
    [closing_percent]                                DECIMAL (26, 6) NULL,
    [delivering_team_member_employee_id]             INT             NULL,
    [delivering_team_member_name]                    VARCHAR (101)   NULL,
    [dim_club_key]                                   VARCHAR (32)    NULL,
    [header_date_range]                              VARCHAR (100)   NULL,
    [header_member_connection_days]                  INT             NULL,
    [number_of_connections]                          INT             NULL,
    [report_date]                                    VARCHAR (33)    NULL,
    [report_date_dim_date_key]                       VARCHAR (8)     NULL,
    [report_date_last_day_in_month_indicator]        VARCHAR (8)     NULL,
    [report_run_date_time]                           VARCHAR (21)    NULL,
    [revenue]                                        DECIMAL (26, 6) NULL,
    [row_label]                                      VARCHAR (13)    NULL,
    [row_label_sort_order]                           INT             NULL,
    [sales_within_14_days_count]                     INT             NULL,
    [dv_load_date_time]                              DATETIME        NULL,
    [dv_load_end_date_time]                          DATETIME        NULL,
    [dv_batch_id]                                    BIGINT          NOT NULL,
    [dv_inserted_date_time]                          DATETIME        NOT NULL,
    [dv_insert_user]                                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                           DATETIME        NULL,
    [dv_update_user]                                 VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

