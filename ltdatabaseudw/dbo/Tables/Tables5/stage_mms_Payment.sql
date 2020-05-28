CREATE TABLE [dbo].[stage_mms_Payment] (
    [stage_mms_Payment_id] BIGINT          NOT NULL,
    [PaymentID]            INT             NULL,
    [ValPaymentTypeID]     TINYINT         NULL,
    [PaymentAmount]        DECIMAL (26, 6) NULL,
    [ApprovalCode]         VARCHAR (50)    NULL,
    [MMSTranID]            INT             NULL,
    [InsertedDateTime]     DATETIME        NULL,
    [UpdatedDateTime]      DATETIME        NULL,
    [TipAmount]            DECIMAL (26, 6) NULL,
    [dv_batch_id]          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

