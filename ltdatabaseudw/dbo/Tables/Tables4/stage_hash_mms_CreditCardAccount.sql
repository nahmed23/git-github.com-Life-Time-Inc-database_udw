CREATE TABLE [dbo].[stage_hash_mms_CreditCardAccount] (
    [stage_hash_mms_CreditCardAccount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [CreditCardAccountID]                 INT          NULL,
    [AccountNumber]                       VARCHAR (19) NULL,
    [Name]                                VARCHAR (50) NULL,
    [ExpirationDate]                      DATETIME     NULL,
    [ValPaymentTypeID]                    TINYINT      NULL,
    [MembershipID]                        INT          NULL,
    [ActiveFlag]                          BIT          NULL,
    [LTFCreditCardAccountFlag]            BIT          NULL,
    [InsertedDateTime]                    DATETIME     NULL,
    [MaskedAccountNumber]                 VARCHAR (17) NULL,
    [UpdatedDateTime]                     DATETIME     NULL,
    [MaskedAccountNumber64]               VARCHAR (17) NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

