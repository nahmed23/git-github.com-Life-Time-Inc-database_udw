CREATE TABLE [dbo].[stage_mms_CreditCardAccount] (
    [stage_mms_CreditCardAccount_id] BIGINT       NOT NULL,
    [CreditCardAccountID]            INT          NULL,
    [AccountNumber]                  VARCHAR (19) NULL,
    [Name]                           VARCHAR (50) NULL,
    [ExpirationDate]                 DATETIME     NULL,
    [ValPaymentTypeID]               TINYINT      NULL,
    [MembershipID]                   INT          NULL,
    [ActiveFlag]                     BIT          NULL,
    [LTFCreditCardAccountFlag]       BIT          NULL,
    [InsertedDateTime]               DATETIME     NULL,
    [MaskedAccountNumber]            VARCHAR (17) NULL,
    [UpdatedDateTime]                DATETIME     NULL,
    [MaskedAccountNumber64]          VARCHAR (17) NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

