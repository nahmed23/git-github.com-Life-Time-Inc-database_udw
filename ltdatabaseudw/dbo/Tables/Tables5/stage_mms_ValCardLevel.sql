CREATE TABLE [dbo].[stage_mms_ValCardLevel] (
    [stage_mms_ValCardLevel_id] BIGINT       NOT NULL,
    [ValCardLevelID]            INT          NULL,
    [Description]               VARCHAR (50) NULL,
    [SortOrder]                 INT          NULL,
    [InsertedDateTime]          DATETIME     NULL,
    [UpdatedDateTime]           DATETIME     NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

