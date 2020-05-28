CREATE TABLE [dbo].[s_lt_bucks_transactions] (
    [s_lt_bucks_transactions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [transaction_id]             INT             NULL,
    [transaction_type]           INT             NULL,
    [transaction_amount]         DECIMAL (26, 6) NULL,
    [transaction_ext_ref]        NVARCHAR (150)  NULL,
    [transaction_int_1]          INT             NULL,
    [transaction_int_2]          INT             NULL,
    [transaction_date_1]         DATETIME        NULL,
    [transaction_timestamp]      DATETIME        NULL,
    [transaction_int_3]          INT             NULL,
    [transaction_int_4]          INT             NULL,
    [transaction_int_5]          INT             NULL,
    [last_modified_timestamp]    DATETIME        NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL,
    [dv_r_load_source_id]        BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_hash]                    CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_lt_bucks_transactions]
    ON [dbo].[s_lt_bucks_transactions]([bk_hash] ASC, [s_lt_bucks_transactions_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_lt_bucks_transactions]([dv_batch_id] ASC);

