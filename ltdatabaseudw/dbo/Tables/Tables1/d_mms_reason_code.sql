CREATE TABLE [dbo].[d_mms_reason_code] (
    [d_mms_reason_code_id]           BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [dim_mms_transaction_reason_key] CHAR (32)    NULL,
    [reason_code_id]                 INT          NULL,
    [description]                    VARCHAR (50) NULL,
    [p_mms_reason_code_id]           BIGINT       NOT NULL,
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
    ON [dbo].[d_mms_reason_code]([dv_batch_id] ASC);

