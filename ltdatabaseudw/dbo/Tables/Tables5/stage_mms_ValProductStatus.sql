CREATE TABLE [dbo].[stage_mms_ValProductStatus] (
    [stage_mms_ValProductStatus_id] BIGINT       NOT NULL,
    [ValProductStatusID]            INT          NULL,
    [Description]                   VARCHAR (50) NULL,
    [SortOrder]                     INT          NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

