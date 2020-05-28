CREATE TABLE [dbo].[stage_mms_ValBusinessArea] (
    [stage_mms_ValBusinessArea_id] BIGINT       NOT NULL,
    [ValBusinessAreaID]            SMALLINT     NULL,
    [Description]                  VARCHAR (50) NULL,
    [SortOrder]                    SMALLINT     NULL,
    [InsertedDatetime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

