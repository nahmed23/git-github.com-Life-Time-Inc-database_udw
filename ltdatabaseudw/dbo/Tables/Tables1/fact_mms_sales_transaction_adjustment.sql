CREATE TABLE [dbo].[fact_mms_sales_transaction_adjustment] (
    [fact_mms_sales_transaction_adjustment_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_club_key]                              CHAR (32)       NULL,
    [dim_mms_drawer_activity_key]               CHAR (32)       NULL,
    [dim_mms_member_key]                        CHAR (32)       NULL,
    [dim_mms_membership_key]                    CHAR (32)       NULL,
    [dim_mms_transaction_reason_key]            CHAR (32)       NULL,
    [fact_mms_sales_transaction_adjustment_key] CHAR (32)       NULL,
    [mms_tran_id]                               INT             NULL,
    [pos_amount]                                DECIMAL (26, 6) NULL,
    [post_dim_date_key]                         CHAR (8)        NULL,
    [tran_amount]                               DECIMAL (26, 6) NULL,
    [tran_dim_date_key]                         CHAR (8)        NULL,
    [tran_item_exists_flag]                     CHAR (1)        NULL,
    [transaction_entered_dim_employee_key]      CHAR (32)       NULL,
    [transaction_reporting_dim_club_key]        CHAR (32)       NULL,
    [udw_inserted_dim_date_key]                 CHAR (8)        NULL,
    [voided_flag]                               CHAR (1)        NULL,
    [dv_load_date_time]                         DATETIME        NULL,
    [dv_load_end_date_time]                     DATETIME        NULL,
    [dv_batch_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_sales_transaction_adjustment_key]));

