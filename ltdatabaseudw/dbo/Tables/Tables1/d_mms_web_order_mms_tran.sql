CREATE TABLE [dbo].[d_mms_web_order_mms_tran] (
    [d_mms_web_order_mms_tran_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [web_order_mms_tran_id]          INT          NULL,
    [fact_mms_sales_transaction_key] CHAR (32)    NULL,
    [mms_web_order_bk_hash]          CHAR (32)    NULL,
    [p_mms_web_order_mms_tran_id]    BIGINT       NOT NULL,
    [dv_load_date_time]              DATETIME     NULL,
    [dv_load_end_date_time]          DATETIME     NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_web_order_mms_tran]([dv_batch_id] ASC);

