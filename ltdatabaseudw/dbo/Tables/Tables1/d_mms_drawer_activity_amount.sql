CREATE TABLE [dbo].[d_mms_drawer_activity_amount] (
    [d_mms_drawer_activity_amount_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_mms_drawer_activity_amount_key] CHAR (32)       NULL,
    [drawer_activity_amount_id]           INT             NULL,
    [actual_total_amount]                 DECIMAL (26, 6) NULL,
    [dim_mms_drawer_activity_key]         CHAR (32)       NULL,
    [payment_type_dim_description_key]    VARCHAR (150)   NULL,
    [r_mms_val_currency_code_bk_hash]     CHAR (32)       NULL,
    [transaction_total_amount]            DECIMAL (26, 6) NULL,
    [p_mms_drawer_activity_amount_id]     BIGINT          NOT NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_drawer_activity_amount]([dv_batch_id] ASC);

