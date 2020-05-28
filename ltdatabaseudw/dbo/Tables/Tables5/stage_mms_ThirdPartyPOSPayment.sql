CREATE TABLE [dbo].[stage_mms_ThirdPartyPOSPayment] (
    [stage_mms_ThirdPartyPOSPayment_id] BIGINT       NOT NULL,
    [ThirdPartyPOSPaymentID]            INT          NULL,
    [ValPaymentStatusID]                TINYINT      NULL,
    [OfflineAuthFlag]                   BIT          NULL,
    [LTFTranDateTime]                   DATETIME     NULL,
    [UTCLTFTranDateTime]                DATETIME     NULL,
    [LTFTranDateTimeZone]               VARCHAR (4)  NULL,
    [POSTranDateTime]                   DATETIME     NULL,
    [UTCPOSTranDateTime]                DATETIME     NULL,
    [POSTranDateTimeZone]               VARCHAR (4)  NULL,
    [POSUniqueTranID]                   VARCHAR (15) NULL,
    [POSUniqueTranIDLabel]              VARCHAR (25) NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

