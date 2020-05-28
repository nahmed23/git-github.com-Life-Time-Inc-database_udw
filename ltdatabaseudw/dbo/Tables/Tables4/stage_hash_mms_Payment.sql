CREATE TABLE [dbo].[stage_hash_mms_Payment] (
    [stage_hash_mms_Payment_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [PaymentID]                 INT             NULL,
    [ValPaymentTypeID]          TINYINT         NULL,
    [PaymentAmount]             DECIMAL (26, 6) NULL,
    [ApprovalCode]              VARCHAR (50)    NULL,
    [MMSTranID]                 INT             NULL,
    [InsertedDateTime]          DATETIME        NULL,
    [UpdatedDateTime]           DATETIME        NULL,
    [TipAmount]                 DECIMAL (26, 6) NULL,
    [dv_load_date_time]         DATETIME        NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

