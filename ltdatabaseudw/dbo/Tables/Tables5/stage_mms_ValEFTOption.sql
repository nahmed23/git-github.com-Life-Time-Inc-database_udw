CREATE TABLE [dbo].[stage_mms_ValEFTOption] (
    [stage_mms_ValEFTOption_id] BIGINT       NOT NULL,
    [ValEFTOptionID]            INT          NULL,
    [Description]               VARCHAR (50) NULL,
    [SortOrder]                 INT          NULL,
    [InsertedDateTime]          DATETIME     NULL,
    [UpdatedDateTime]           DATETIME     NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

