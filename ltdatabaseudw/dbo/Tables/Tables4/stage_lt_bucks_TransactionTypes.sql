CREATE TABLE [dbo].[stage_lt_bucks_TransactionTypes] (
    [stage_lt_bucks_TransactionTypes_id] BIGINT       NOT NULL,
    [ttype_id]                           INT          NULL,
    [ttype_desc]                         VARCHAR (50) NULL,
    [LastModifiedTimestamp]              DATETIME     NULL,
    [dv_batch_id]                        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

