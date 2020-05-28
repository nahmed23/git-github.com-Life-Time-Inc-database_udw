CREATE TABLE [dbo].[stage_mms_ValDiscountType] (
    [stage_mms_ValDiscountType_id] BIGINT       NOT NULL,
    [ValDiscountTypeID]            INT          NULL,
    [Description]                  VARCHAR (50) NULL,
    [SortOrder]                    INT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

