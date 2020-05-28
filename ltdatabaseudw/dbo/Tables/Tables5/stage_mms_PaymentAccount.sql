CREATE TABLE [dbo].[stage_mms_PaymentAccount] (
    [stage_mms_PaymentAccount_id] BIGINT       NOT NULL,
    [PaymentAccountID]            INT          NULL,
    [PaymentID]                   INT          NULL,
    [ExpirationDate]              DATETIME     NULL,
    [AccountNumber]               VARCHAR (19) NULL,
    [Name]                        VARCHAR (50) NULL,
    [InsertedDateTime]            DATETIME     NULL,
    [RoutingNumber]               VARCHAR (9)  NULL,
    [BankName]                    VARCHAR (50) NULL,
    [MaskedAccountNumber]         VARCHAR (17) NULL,
    [UpdatedDateTime]             DATETIME     NULL,
    [MaskedAccountNumber64]       VARCHAR (17) NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

