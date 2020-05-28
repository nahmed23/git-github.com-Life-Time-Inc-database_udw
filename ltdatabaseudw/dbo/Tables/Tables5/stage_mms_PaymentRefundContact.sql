CREATE TABLE [dbo].[stage_mms_PaymentRefundContact] (
    [stage_mms_PaymentRefundContact_id] BIGINT       NOT NULL,
    [PaymentRefundContactID]            INT          NULL,
    [FirstName]                         VARCHAR (50) NULL,
    [LastName]                          VARCHAR (50) NULL,
    [MiddleInit]                        CHAR (1)     NULL,
    [PhoneAreaCode]                     VARCHAR (3)  NULL,
    [PhoneNumber]                       VARCHAR (7)  NULL,
    [AddressLine1]                      VARCHAR (50) NULL,
    [AddressLine2]                      VARCHAR (50) NULL,
    [City]                              VARCHAR (50) NULL,
    [Zip]                               VARCHAR (11) NULL,
    [ValCountryID]                      TINYINT      NULL,
    [ValStateID]                        SMALLINT     NULL,
    [PaymentRefundID]                   INT          NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

