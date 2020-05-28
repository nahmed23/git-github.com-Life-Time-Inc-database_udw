CREATE TABLE [dbo].[stage_hash_mms_GLAccount] (
    [stage_hash_mms_GLAccount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [GLAccountID]                 INT          NULL,
    [RevenueGLAccountNumber]      VARCHAR (10) NULL,
    [RefundGLAccountNumber]       VARCHAR (10) NULL,
    [InsertedDateTime]            DATETIME     NULL,
    [UpdatedDateTime]             DATETIME     NULL,
    [DiscountGLAccount]           VARCHAR (10) NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

