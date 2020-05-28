﻿CREATE TABLE [dbo].[stage_mms_PTCreditCardUndeliverableTransaction] (
    [stage_mms_PTCreditCardUndeliverableTransaction_id] BIGINT          NOT NULL,
    [PTCreditCardUndeliverableTransactionID]            INT             NULL,
    [EntryDataSource]                                   SMALLINT        NULL,
    [AccountNumber]                                     VARCHAR (19)    NULL,
    [ExpirationDate]                                    DATETIME        NULL,
    [TranAmount]                                        NUMERIC (10, 3) NULL,
    [ReferenceCode]                                     VARCHAR (6)     NULL,
    [TipAmount]                                         NUMERIC (10, 3) NULL,
    [EmployeeID]                                        INT             NULL,
    [MemberID]                                          INT             NULL,
    [CardHolderStreetAddress]                           VARCHAR (20)    NULL,
    [CardHolderZipCode]                                 VARCHAR (9)     NULL,
    [TransactionDateTime]                               DATETIME        NULL,
    [UTCTransactionDateTime]                            DATETIME        NULL,
    [TransactionDateTimeZone]                           VARCHAR (4)     NULL,
    [IndustryCode]                                      SMALLINT        NULL,
    [ReasonMessage]                                     VARCHAR (260)   NULL,
    [CardType]                                          VARCHAR (3)     NULL,
    [PTCreditCardTerminalID]                            INT             NULL,
    [CardOnFileFlag]                                    BIT             NULL,
    [InsertedDateTime]                                  DATETIME        NULL,
    [MaskedAccountNumber]                               VARCHAR (17)    NULL,
    [UpdatedDateTime]                                   DATETIME        NULL,
    [MaskedAccountNumber64]                             VARCHAR (17)    NULL,
    [CardHolderName]                                    VARCHAR (50)    NULL,
    [TypeIndicator]                                     INT             NULL,
    [ThirdPartyPOSPaymentID]                            INT             NULL,
    [HbcPaymentFlag]                                    BIT             NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
