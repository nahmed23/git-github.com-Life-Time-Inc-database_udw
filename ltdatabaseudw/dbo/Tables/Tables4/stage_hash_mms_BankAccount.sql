CREATE TABLE [dbo].[stage_hash_mms_BankAccount] (
    [stage_hash_mms_BankAccount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [BankAccountID]                 INT          NULL,
    [MembershipID]                  INT          NULL,
    [AccountNumber]                 VARCHAR (17) NULL,
    [Name]                          VARCHAR (50) NULL,
    [PreNotifyFlag]                 BIT          NULL,
    [RoutingNumber]                 VARCHAR (9)  NULL,
    [ValPaymentTypeID]              TINYINT      NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [BankName]                      VARCHAR (50) NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [MaskedAccountNumber]           VARCHAR (17) NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

