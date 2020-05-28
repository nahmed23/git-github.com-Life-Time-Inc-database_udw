CREATE TABLE [dbo].[fact_mms_drawer_activity_amount] (
    [fact_mms_drawer_activity_amount_id]                   BIGINT          IDENTITY (1, 1) NOT NULL,
    [fact_mms_drawer_activity_amount_key]                  CHAR (32)       NULL,
    [actual_total_amount]                                  DECIMAL (26, 6) NULL,
    [dim_club_currency_code_key]                           CHAR (32)       NULL,
    [dim_club_key]                                         CHAR (32)       NULL,
    [dim_merchant_number_key]                              CHAR (32)       NULL,
    [dim_mms_drawer_activity_key]                          CHAR (32)       NULL,
    [drawer_activity_amount_id]                            INT             NULL,
    [local_currency_dim_plan_exchange_rate_key]            CHAR (32)       NULL,
    [local_currency_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [original_currency_code]                               CHAR (15)       NULL,
    [payment_type_dim_description_key]                     NVARCHAR (100)  NULL,
    [transaction_total_amount]                             DECIMAL (26, 6) NULL,
    [usd_dim_plan_exchange_rate_key]                       CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key]            CHAR (32)       NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_drawer_activity_amount_key]));

