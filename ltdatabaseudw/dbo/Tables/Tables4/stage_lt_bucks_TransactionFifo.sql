CREATE TABLE [dbo].[stage_lt_bucks_TransactionFifo] (
    [stage_lt_bucks_TransactionFifo_id] BIGINT          NOT NULL,
    [tfifo_id]                          INT             NULL,
    [tfifo_transaction1]                INT             NULL,
    [tfifo_transaction2]                INT             NULL,
    [tfifo_amount]                      DECIMAL (26, 6) NULL,
    [tfifo_timestamp]                   DATETIME        NULL,
    [LastModifiedTimestamp]             DATETIME        NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

