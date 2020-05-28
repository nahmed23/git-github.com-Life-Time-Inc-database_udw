CREATE TABLE [dbo].[fact_mms_new_membership] (
    [fact_mms_new_membership_id]                           BIGINT          IDENTITY (1, 1) NOT NULL,
    [corporate_membership_flag]                            CHAR (1)        NULL,
    [created_date_time_key]                                CHAR (8)        NULL,
    [dim_mms_member_key]                                   CHAR (32)       NULL,
    [dim_mms_membership_type_key]                          CHAR (32)       NULL,
    [enrollment_fee]                                       DECIMAL (26, 6) NULL,
    [fact_mms_new_membership_key]                          CHAR (32)       NULL,
    [home_dim_club_key]                                    CHAR (32)       NULL,
    [include_in_dssr_flag]                                 CHAR (1)        NULL,
    [local_currency_dim_plan_exchange_rate_key]            CHAR (32)       NULL,
    [local_currency_monthly_average_dim_exchange_rate_key] CHAR (32)       NULL,
    [membership_id]                                        INT             NULL,
    [original_currency_code]                               CHAR (3)        NULL,
    [primary_sales_dim_employee_key]                       CHAR (32)       NULL,
    [usd_dim_plan_exchange_rate_key]                       CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key]            CHAR (32)       NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_new_membership_key]));

