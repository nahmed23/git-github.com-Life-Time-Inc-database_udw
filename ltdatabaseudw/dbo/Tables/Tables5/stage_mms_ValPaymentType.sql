CREATE TABLE [dbo].[stage_mms_ValPaymentType] (
    [stage_mms_ValPaymentType_id] BIGINT       NOT NULL,
    [ValPaymentTypeID]            INT          NULL,
    [Description]                 VARCHAR (50) NULL,
    [SortOrder]                   INT          NULL,
    [ValEFTAccountTypeID]         INT          NULL,
    [ViewPaymentTypeFlag]         BIT          NULL,
    [ViewBankAccountTypeFlag]     BIT          NULL,
    [InsertedDateTime]            DATETIME     NULL,
    [UpdatedDateTime]             DATETIME     NULL,
    [RequiresPaymentTerminalFlag] BIT          NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

