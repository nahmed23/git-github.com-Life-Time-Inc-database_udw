CREATE TABLE [dbo].[stage_mms_PaymentRefund] (
    [stage_mms_PaymentRefund_id] BIGINT        NOT NULL,
    [PaymentRefundID]            INT           NULL,
    [PaymentID]                  INT           NULL,
    [ValPaymentStatusID]         TINYINT       NULL,
    [StatusChangeDateTime]       DATETIME      NULL,
    [UTCStatusChangeDateTime]    DATETIME      NULL,
    [StatusChangeDateTimeZone]   VARCHAR (4)   NULL,
    [StatusChangeEmployeeID]     INT           NULL,
    [PaymentIssuedDateTime]      DATETIME      NULL,
    [Comment]                    VARCHAR (255) NULL,
    [ReferenceNumber]            VARCHAR (50)  NULL,
    [InsertedDateTime]           DATETIME      NULL,
    [UpdatedDateTime]            DATETIME      NULL,
    [dv_batch_id]                BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

