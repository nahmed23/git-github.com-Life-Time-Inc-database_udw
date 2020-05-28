CREATE TABLE [dbo].[fact_ptdssr_old_and_new_business_employee_summary] (
    [fact_ptdssr_old_and_new_business_employee_summary_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [business_sub_type]                                    VARCHAR (50)    NULL,
    [business_type]                                        VARCHAR (50)    NULL,
    [dim_club_key]                                         VARCHAR (32)    NULL,
    [dim_employee_key]                                     VARCHAR (32)    NULL,
    [employee_id]                                          INT             NULL,
    [forecast_amount]                                      DECIMAL (26, 2) NULL,
    [mms_club_id]                                          VARCHAR (400)   NULL,
    [month_to_date_revenue_item_amount]                    DECIMAL (26, 2) NULL,
    [report_date_dim_date_key]                             VARCHAR (8)     NULL,
    [report_date_is_last_day_in_month_indicator]           VARCHAR (8)     NULL,
    [report_date_item_amount]                              DECIMAL (26, 2) NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

