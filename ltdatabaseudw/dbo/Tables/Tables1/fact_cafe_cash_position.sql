CREATE TABLE [dbo].[fact_cafe_cash_position] (
    [fact_cafe_cash_position_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [accountable_cash]                DECIMAL (26, 6) NULL,
    [cash_drop_amount]                DECIMAL (26, 6) NULL,
    [cashier_dim_cafe_employee_key]   VARCHAR (32)    NULL,
    [dim_cafe_business_day_dates_key] VARCHAR (32)    NULL,
    [dim_cafe_meal_period_key]        VARCHAR (32)    NULL,
    [dim_cafe_profit_center_key]      VARCHAR (32)    NULL,
    [fact_cafe_cash_position_key]     VARCHAR (32)    NULL,
    [loan_amount]                     DECIMAL (26, 6) NULL,
    [net_cash_tender_amount]          DECIMAL (26, 6) NULL,
    [over_short_amount]               DECIMAL (26, 6) NULL,
    [paid_tips]                       DECIMAL (26, 6) NULL,
    [withdrawal_amount]               DECIMAL (26, 6) NULL,
    [dv_load_date_time]               DATETIME        NULL,
    [dv_load_end_date_time]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_cafe_cash_position_key]));

