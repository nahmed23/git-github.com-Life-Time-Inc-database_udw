CREATE TABLE [dbo].[stage_mms_ValChildCenterUsageException] (
    [stage_mms_ValChildCenterUsageException_id] BIGINT       NOT NULL,
    [ValChildCenterUsageExceptionID]            SMALLINT     NULL,
    [Description]                               VARCHAR (50) NULL,
    [SortOrder]                                 SMALLINT     NULL,
    [InsertedDatetime]                          DATETIME     NULL,
    [UpdatedDateTime]                           DATETIME     NULL,
    [dv_batch_id]                               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

