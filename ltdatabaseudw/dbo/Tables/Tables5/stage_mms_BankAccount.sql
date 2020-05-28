CREATE TABLE [dbo].[stage_mms_BankAccount] (
    [stage_mms_BankAccount_id] BIGINT       NOT NULL,
    [BankAccountID]            INT          NULL,
    [MembershipID]             INT          NULL,
    [AccountNumber]            VARCHAR (17) NULL,
    [Name]                     VARCHAR (50) NULL,
    [PreNotifyFlag]            BIT          NULL,
    [RoutingNumber]            VARCHAR (9)  NULL,
    [ValPaymentTypeID]         TINYINT      NULL,
    [InsertedDateTime]         DATETIME     NULL,
    [BankName]                 VARCHAR (50) NULL,
    [UpdatedDateTime]          DATETIME     NULL,
    [MaskedAccountNumber]      VARCHAR (17) NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

