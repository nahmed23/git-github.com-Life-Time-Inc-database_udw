CREATE TABLE [dbo].[fact_mms_membership_balance_history] (
    [fact_mms_membership_balance_history_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_mms_membership_key]                    CHAR (32)       NULL,
    [membership_id]                             INT             NULL,
    [effective_date_time]                       DATETIME        NULL,
    [expiration_date_time]                      DATETIME        NULL,
    [committed_balance_products]                NUMERIC (12, 2) NULL,
    [current_balance_products]                  NUMERIC (12, 2) NULL,
    [end_of_day_committed_balance]              NUMERIC (12, 2) NULL,
    [end_of_day_current_balance]                NUMERIC (12, 2) NULL,
    [end_of_day_statement_balance]              NUMERIC (12, 2) NULL,
    [membership_balance_id]                     INT             NULL,
    [original_currency_code]                    VARCHAR (15)    NULL,
    [processing_complete_flag]                  CHAR (1)        NULL,
    [usd_dim_plan_exchange_rate_key]            CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [dv_load_date_time]                         DATETIME        NULL,
    [dv_load_end_date_time]                     DATETIME        NULL,
    [dv_batch_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

