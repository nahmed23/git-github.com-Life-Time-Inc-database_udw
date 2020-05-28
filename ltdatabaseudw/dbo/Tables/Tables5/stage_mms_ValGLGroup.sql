CREATE TABLE [dbo].[stage_mms_ValGLGroup] (
    [stage_mms_ValGLGroup_id] BIGINT       NOT NULL,
    [ValGLGroupID]            INT          NULL,
    [Description]             VARCHAR (50) NULL,
    [SortOrder]               INT          NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

