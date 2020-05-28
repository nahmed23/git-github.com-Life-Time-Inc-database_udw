CREATE TABLE [dbo].[stage_mms_ValMessageSeverity] (
    [stage_mms_ValMessageSeverity_id] BIGINT       NOT NULL,
    [ValMessageSeverityID]            INT          NULL,
    [Description]                     VARCHAR (50) NULL,
    [SortOrder]                       INT          NULL,
    [SeverityLevel]                   INT          NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

