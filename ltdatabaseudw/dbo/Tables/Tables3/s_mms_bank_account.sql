CREATE TABLE [dbo].[s_mms_bank_account] (
    [s_mms_bank_account_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [bank_account_id]       INT          NULL,
    [account_number]        VARCHAR (17) NULL,
    [name]                  VARCHAR (50) NULL,
    [pre_notify_flag]       BIT          NULL,
    [routing_number]        VARCHAR (9)  NULL,
    [inserted_date_time]    DATETIME     NULL,
    [bank_name]             VARCHAR (50) NULL,
    [updated_date_time]     DATETIME     NULL,
    [masked_account_number] VARCHAR (17) NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_bank_account]
    ON [dbo].[s_mms_bank_account]([bk_hash] ASC, [s_mms_bank_account_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_bank_account]([dv_batch_id] ASC);

