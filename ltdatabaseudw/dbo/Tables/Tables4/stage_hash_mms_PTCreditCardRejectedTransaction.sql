﻿CREATE TABLE [dbo].[stage_hash_mms_PTCreditCardRejectedTransaction] (
    [stage_hash_mms_PTCreditCardRejectedTransaction_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [PTCreditCardRejectedTransactionID]                 INT             NULL,
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
    [AuthorizationNetWorkID]                            TINYINT         NULL,
    [AuthorizationSource]                               CHAR (1)        NULL,
    [ErrorCode]                                         VARCHAR (6)     NULL,
    [ErrorMessage]                                      VARCHAR (50)    NULL,
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
    [dv_load_date_time]                                 DATETIME        NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
