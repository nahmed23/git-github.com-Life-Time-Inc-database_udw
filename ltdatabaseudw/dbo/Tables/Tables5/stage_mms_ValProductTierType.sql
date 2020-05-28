CREATE TABLE [dbo].[stage_mms_ValProductTierType] (
    [stage_mms_ValProductTierType_id] BIGINT       NOT NULL,
    [ValProductTierTypeID]            INT          NULL,
    [Description]                     VARCHAR (50) NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

