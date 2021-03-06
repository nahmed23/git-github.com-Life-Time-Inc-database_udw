﻿CREATE TABLE [dbo].[fact_mms_package_session] (
    [fact_mms_package_session_id]                                    BIGINT          IDENTITY (1, 1) NOT NULL,
    [body_age_assessment_count]                                      INT             NULL,
    [created_dim_date_key]                                           CHAR (8)        NULL,
    [created_dim_time_key]                                           CHAR (8)        NULL,
    [delivered_dim_club_key]                                         CHAR (32)       NULL,
    [delivered_dim_date_key]                                         CHAR (8)        NULL,
    [delivered_dim_employee_key]                                     CHAR (32)       NULL,
    [delivered_dim_time_key]                                         CHAR (8)        NULL,
    [delivered_session_discount_value]                               DECIMAL (26, 6) NULL,
    [delivered_session_lt_bucks_amount]                              DECIMAL (26, 6) NULL,
    [delivered_session_price]                                        DECIMAL (26, 6) NULL,
    [dim_mms_member_key]                                             CHAR (32)       NULL,
    [edw_inserted_dim_date_key]                                      CHAR (32)       NULL,
    [fact_mms_package_dim_product_key]                               CHAR (32)       NULL,
    [fact_mms_package_key]                                           CHAR (32)       NULL,
    [fact_mms_package_session_key]                                   CHAR (32)       NULL,
    [fact_mms_sales_transaction_key]                                 CHAR (32)       NULL,
    [mms_tran_id]                                                    INT             NULL,
    [number_of_sessions_in_package]                                  INT             NULL,
    [original_currency_code]                                         CHAR (3)        NULL,
    [package_created_dim_date_key]                                   CHAR (8)        NULL,
    [package_created_dim_time_key]                                   CHAR (8)        NULL,
    [package_edited_flag]                                            CHAR (1)        NULL,
    [package_entered_dim_club_key]                                   CHAR (32)       NULL,
    [package_entered_dim_employee_key]                               CHAR (32)       NULL,
    [package_id]                                                     INT             NULL,
    [package_session_id]                                             INT             NULL,
    [package_status_dim_description_key]                             CHAR (255)      NULL,
    [primary_sales_dim_employee_key]                                 CHAR (32)       NULL,
    [reporting_dim_club_key]                                         CHAR (32)       NULL,
    [reporting_local_currency_dim_plan_exchange_rate_key]            CHAR (32)       NULL,
    [reporting_local_currency_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [sales_channel_dim_description_key]                              CHAR (255)      NULL,
    [secondary_sales_dim_employee_key]                               CHAR (32)       NULL,
    [session_comment]                                                VARCHAR (255)   NULL,
    [session_complete_count]                                         INT             NULL,
    [tran_item_id]                                                   INT             NULL,
    [usd_dim_plan_exchange_rate_key]                                 CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key]                      CHAR (32)       NULL,
    [voided_flag]                                                    CHAR (1)        NULL,
    [dv_load_date_time]                                              DATETIME        NULL,
    [dv_load_end_date_time]                                          DATETIME        NULL,
    [dv_batch_id]                                                    BIGINT          NOT NULL,
    [dv_inserted_date_time]                                          DATETIME        NOT NULL,
    [dv_insert_user]                                                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                           DATETIME        NULL,
    [dv_update_user]                                                 VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_mms_package_session_key]));

