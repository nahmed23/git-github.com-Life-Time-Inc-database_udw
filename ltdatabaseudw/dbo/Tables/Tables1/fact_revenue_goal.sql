CREATE TABLE [dbo].[fact_revenue_goal] (
    [fact_revenue_goal_id]                                 BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_club_key]                                         CHAR (32)       NULL,
    [dim_reporting_hierarchy_key]                          CHAR (32)       NULL,
    [goal_dollar_amount]                                   DECIMAL (12, 2) NULL,
    [goal_effective_dim_date_key]                          CHAR (8)        NULL,
    [local_currency_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [original_currency_code]                               CHAR (3)        NULL,
    [usd_monthly_average_dim_exchange_rate_key]            CHAR (32)       NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

