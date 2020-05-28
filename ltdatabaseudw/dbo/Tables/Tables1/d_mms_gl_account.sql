CREATE TABLE [dbo].[d_mms_gl_account] (
    [d_mms_gl_account_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [gl_account_id]             INT          NULL,
    [discount_gl_account]       CHAR (10)    NULL,
    [refund_gl_account_number]  CHAR (10)    NULL,
    [revenue_gl_account_number] CHAR (10)    NULL,
    [p_mms_gl_account_id]       BIGINT       NOT NULL,
    [dv_load_date_time]         DATETIME     NULL,
    [dv_load_end_date_time]     DATETIME     NULL,
    [dv_batch_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_gl_account]([dv_batch_id] ASC);

