CREATE TABLE [dbo].[stage_hash_mms_ClubGLAccount] (
    [stage_hash_mms_ClubGLAccount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [ClubGLAccountID]                 INT          NULL,
    [ClubID]                          INT          NULL,
    [ValCurrencyCodeID]               TINYINT      NULL,
    [GLCashEntryCompanyName]          VARCHAR (15) NULL,
    [GLCashEntryAccount]              VARCHAR (4)  NULL,
    [GLReceivablesEntryAccount]       VARCHAR (4)  NULL,
    [GLCashEntryCashSubAccount]       VARCHAR (11) NULL,
    [GLCashEntryCreditCardSubAccount] VARCHAR (11) NULL,
    [GLReceivablesEntrySubAccount]    VARCHAR (11) NULL,
    [GLReceivablesEntryCompanyName]   VARCHAR (15) NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

