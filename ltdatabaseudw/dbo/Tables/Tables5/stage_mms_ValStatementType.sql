CREATE TABLE [dbo].[stage_mms_ValStatementType] (
    [stage_mms_ValStatementType_id] BIGINT       NOT NULL,
    [ValStatementTypeID]            INT          NULL,
    [Description]                   VARCHAR (50) NULL,
    [SortOrder]                     INT          NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

