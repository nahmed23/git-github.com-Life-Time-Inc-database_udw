CREATE TABLE [dbo].[stage_mms_ValMIPCategory] (
    [stage_mms_ValMIPCategory_id] BIGINT       NOT NULL,
    [ValMIPCategoryID]            SMALLINT     NULL,
    [Description]                 VARCHAR (50) NULL,
    [SortOrder]                   SMALLINT     NULL,
    [InsertedDateTime]            DATETIME     NULL,
    [UpdatedDateTime]             DATETIME     NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

