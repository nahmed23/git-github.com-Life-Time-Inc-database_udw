CREATE TABLE [dbo].[d_mms_tran_item_discount] (
    [d_mms_tran_item_discount_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [fact_mms_sales_transaction_discount_key] CHAR (32)       NULL,
    [tran_item_discount_id]                   INT             NULL,
    [applied_discount_amount]                 DECIMAL (26, 6) NULL,
    [inserted_date_time]                      DATETIME        NULL,
    [pricing_discount_id]                     INT             NULL,
    [promotion_code]                          VARCHAR (50)    NULL,
    [tran_item_id]                            INT             NULL,
    [val_discount_reason_id]                  SMALLINT        NULL,
    [p_mms_tran_item_discount_id]             BIGINT          NOT NULL,
    [deleted_flag]                            INT             NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_tran_item_discount]([dv_batch_id] ASC);

