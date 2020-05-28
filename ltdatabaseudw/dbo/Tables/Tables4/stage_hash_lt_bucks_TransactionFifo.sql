CREATE TABLE [dbo].[stage_hash_lt_bucks_TransactionFifo] (
    [stage_hash_lt_bucks_TransactionFifo_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [tfifo_id]                               INT             NULL,
    [tfifo_transaction1]                     INT             NULL,
    [tfifo_transaction2]                     INT             NULL,
    [tfifo_amount]                           DECIMAL (26, 6) NULL,
    [tfifo_timestamp]                        DATETIME        NULL,
    [LastModifiedTimestamp]                  DATETIME        NULL,
    [dv_load_date_time]                      DATETIME        NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL,
    [dv_batch_id]                            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

