CREATE TABLE [dbo].[stage_hash_lt_bucks_Transactions] (
    [stage_hash_lt_bucks_Transactions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [transaction_id]                      INT             NULL,
    [transaction_type]                    INT             NULL,
    [transaction_user]                    INT             NULL,
    [transaction_amount]                  DECIMAL (26, 6) NULL,
    [transaction_session]                 INT             NULL,
    [transaction_ext_ref]                 NVARCHAR (150)  NULL,
    [transaction_int1]                    INT             NULL,
    [transaction_int2]                    INT             NULL,
    [transaction_date1]                   DATETIME        NULL,
    [transaction_timestamp]               DATETIME        NULL,
    [transaction_promotion]               INT             NULL,
    [transaction_int3]                    INT             NULL,
    [transaction_int4]                    INT             NULL,
    [transaction_int5]                    INT             NULL,
    [LastModifiedTimestamp]               DATETIME        NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

