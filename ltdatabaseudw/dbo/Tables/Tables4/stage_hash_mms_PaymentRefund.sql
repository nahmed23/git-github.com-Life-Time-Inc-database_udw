CREATE TABLE [dbo].[stage_hash_mms_PaymentRefund] (
    [stage_hash_mms_PaymentRefund_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [PaymentRefundID]                 INT           NULL,
    [PaymentID]                       INT           NULL,
    [ValPaymentStatusID]              TINYINT       NULL,
    [StatusChangeDateTime]            DATETIME      NULL,
    [UTCStatusChangeDateTime]         DATETIME      NULL,
    [StatusChangeDateTimeZone]        VARCHAR (4)   NULL,
    [StatusChangeEmployeeID]          INT           NULL,
    [PaymentIssuedDateTime]           DATETIME      NULL,
    [Comment]                         VARCHAR (255) NULL,
    [ReferenceNumber]                 VARCHAR (50)  NULL,
    [InsertedDateTime]                DATETIME      NULL,
    [UpdatedDateTime]                 DATETIME      NULL,
    [dv_load_date_time]               DATETIME      NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

