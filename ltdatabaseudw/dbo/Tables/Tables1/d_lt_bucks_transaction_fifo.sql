CREATE TABLE [dbo].[d_lt_bucks_transaction_fifo] (
    [d_lt_bucks_transaction_fifo_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_lt_bucks_award_spend_key]       CHAR (32)       NULL,
    [tfifo_id]                            INT             NULL,
    [award_fact_lt_bucks_transaction_key] CHAR (32)       NULL,
    [bucks_amount]                        DECIMAL (26, 6) NULL,
    [spend_fact_lt_bucks_transaction_key] CHAR (32)       NULL,
    [transaction_date_time]               DATETIME        NULL,
    [p_lt_bucks_transaction_fifo_id]      BIGINT          NOT NULL,
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
    ON [dbo].[d_lt_bucks_transaction_fifo]([dv_batch_id] ASC);

