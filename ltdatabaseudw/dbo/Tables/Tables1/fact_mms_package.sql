﻿CREATE TABLE [dbo].[fact_mms_package] (
    [fact_mms_package_id]                                            BIGINT          IDENTITY (1, 1) NOT NULL,
    [balance_amount]                                                 DECIMAL (26, 6) NULL,
    [balance_amount_less_lt_bucks]                                   DECIMAL (26, 6) NULL,
    [created_dim_date_key]                                           CHAR (8)        NULL,
    [created_dim_time_key]                                           CHAR (8)        NULL,
    [dim_club_key]                                                   CHAR (32)       NULL,
    [dim_mms_member_key]                                             CHAR (32)       NULL,
    [dim_mms_product_key]                                            CHAR (32)       NULL,
    [external_package_id]                                            VARCHAR (50)    NULL,
    [fact_mms_package_key]                                           CHAR (32)       NULL,
    [fact_mms_sales_transaction_key]                                 CHAR (32)       NULL,
    [inserted_date_time]                                             DATETIME        NULL,
    [inserted_dim_date_key]                                          CHAR (8)        NULL,
    [inserted_dim_time_key]                                          CHAR (8)        NULL,
    [item_lt_bucks_amount]                                           DECIMAL (26, 6) NULL,
    [mms_tran_id]                                                    INT             NULL,
    [number_of_sessions]                                             SMALLINT        NULL,
    [original_currency_code]                                         CHAR (3)        NULL,
    [package_edit_dim_date_key]                                      CHAR (8)        NULL,
    [package_edit_dim_time_key]                                      CHAR (8)        NULL,
    [package_edited_flag]                                            CHAR (1)        NULL,
    [package_entered_dim_employee_key]                               CHAR (32)       NULL,
    [package_id]                                                     INT             NULL,
    [package_status_dim_description_key]                             VARCHAR (255)   NULL,
    [price_per_session]                                              DECIMAL (26, 6) NULL,
    [price_per_session_less_lt_bucks]                                DECIMAL (26, 6) NULL,
    [primary_sales_dim_employee_key]                                 CHAR (32)       NULL,
    [reporting_dim_club_key]                                         CHAR (32)       NULL,
    [reporting_local_currency_dim_plan_exchange_rate_key]            CHAR (32)       NULL,
    [reporting_local_currency_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [sales_channel_dim_description_key]                              CHAR (255)      NULL,
    [sales_discount_amount]                                          DECIMAL (26, 6) NULL,
    [secondary_sales_dim_employee_key]                               CHAR (32)       NULL,
    [sessions_left]                                                  INT             NULL,
    [tran_item_id]                                                   INT             NULL,
    [transaction_post_dim_date_key]                                  CHAR (8)        NULL,
    [transaction_source]                                             VARCHAR (50)    NULL,
    [transaction_void_flag]                                          CHAR (1)        NULL,
    [updated_date_time]                                              DATETIME        NULL,
    [updated_dim_date_key]                                           CHAR (8)        NULL,
    [updated_dim_time_key]                                           CHAR (8)        NULL,
    [usd_dim_plan_exchange_rate_key]                                 CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key]                      CHAR (32)       NULL,
    [dv_load_date_time]                                              DATETIME        NULL,
    [dv_load_end_date_time]                                          DATETIME        NULL,
    [dv_batch_id]                                                    BIGINT          NOT NULL,
    [dv_inserted_date_time]                                          DATETIME        NOT NULL,
    [dv_insert_user]                                                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                           DATETIME        NULL,
    [dv_update_user]                                                 VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_mms_package_key]));
