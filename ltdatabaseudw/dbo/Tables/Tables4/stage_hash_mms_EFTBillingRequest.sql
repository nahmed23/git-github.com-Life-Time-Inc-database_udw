CREATE TABLE [dbo].[stage_hash_mms_EFTBillingRequest] (
    [stage_hash_mms_EFTBillingRequest_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)     NOT NULL,
    [EFTBillingRequestID]                 INT           NULL,
    [FileName]                            VARCHAR (50)  NULL,
    [ClubID]                              VARCHAR (20)  NULL,
    [PersonID]                            VARCHAR (20)  NULL,
    [ProductID]                           VARCHAR (20)  NULL,
    [ProductPrice]                        VARCHAR (20)  NULL,
    [Quantity]                            VARCHAR (20)  NULL,
    [TotalAmount]                         VARCHAR (20)  NULL,
    [PaymentRequestReference]             VARCHAR (50)  NULL,
    [CommissionEmployee]                  VARCHAR (20)  NULL,
    [TransactionSource]                   VARCHAR (20)  NULL,
    [ExternalItemID]                      VARCHAR (20)  NULL,
    [ExternalPackageID]                   VARCHAR (50)  NULL,
    [OriginalExternalItemID]              VARCHAR (20)  NULL,
    [SubscriptionID]                      VARCHAR (50)  NULL,
    [mmsTranID]                           INT           NULL,
    [PackageID]                           INT           NULL,
    [ResponseCode]                        VARCHAR (20)  NULL,
    [Message]                             VARCHAR (120) NULL,
    [InsertedDateTime]                    DATETIME      NULL,
    [UpdatedDateTime]                     DATETIME      NULL,
    [dv_load_date_time]                   DATETIME      NOT NULL,
    [dv_batch_id]                         BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

