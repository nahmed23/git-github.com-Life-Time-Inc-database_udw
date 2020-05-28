CREATE TABLE [dbo].[d_lt_bucks_transactions] (
    [d_lt_bucks_transactions_id]                                   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                      CHAR (32)       NOT NULL,
    [fact_lt_bucks_transactions_key]                               CHAR (32)       NULL,
    [transaction_id]                                               BIGINT          NULL,
    [award_reason]                                                 NVARCHAR (150)  NULL,
    [bucks_amount]                                                 DECIMAL (26, 6) NULL,
    [bucks_expiration_date_time]                                   DATETIME        NULL,
    [cancelled_order_original_fact_mylt_bucks_transaction_id]      INT             NULL,
    [cancelled_order_original_fact_mylt_bucks_transaction_item_id] INT             NULL,
    [dim_lt_bucks_user_key]                                        CHAR (32)       NULL,
    [pended_date_time]                                             DATETIME        NULL,
    [transaction_date_time]                                        DATETIME        NULL,
    [transaction_type_id]                                          INT             NULL,
    [p_lt_bucks_transactions_id]                                   BIGINT          NOT NULL,
    [dv_load_date_time]                                            DATETIME        NULL,
    [dv_load_end_date_time]                                        DATETIME        NULL,
    [dv_batch_id]                                                  BIGINT          NOT NULL,
    [dv_inserted_date_time]                                        DATETIME        NOT NULL,
    [dv_insert_user]                                               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                         DATETIME        NULL,
    [dv_update_user]                                               VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_transactions]([dv_batch_id] ASC);

