CREATE TABLE [dbo].[stage_lt_bucks_Transactions] (
    [stage_lt_bucks_Transactions_id] BIGINT          NOT NULL,
    [transaction_id]                 INT             NULL,
    [transaction_type]               INT             NULL,
    [transaction_user]               INT             NULL,
    [transaction_amount]             DECIMAL (26, 6) NULL,
    [transaction_session]            INT             NULL,
    [transaction_ext_ref]            NVARCHAR (150)  NULL,
    [transaction_int1]               INT             NULL,
    [transaction_int2]               INT             NULL,
    [transaction_date1]              DATETIME        NULL,
    [transaction_timestamp]          DATETIME        NULL,
    [transaction_promotion]          INT             NULL,
    [transaction_int3]               INT             NULL,
    [transaction_int4]               INT             NULL,
    [transaction_int5]               INT             NULL,
    [LastModifiedTimestamp]          DATETIME        NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

