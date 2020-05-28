CREATE TABLE [dbo].[stage_mms_ChildCenterUsageActivityArea] (
    [stage_mms_ChildCenterUsageActivityArea_id] BIGINT   NOT NULL,
    [ChildCenterUsageActivityAreaID]            INT      NULL,
    [ChildCenterUsageID]                        INT      NULL,
    [ValActivityAreaID]                         SMALLINT NULL,
    [InsertedDatetime]                          DATETIME NULL,
    [UpdatedDateTime]                           DATETIME NULL,
    [dv_batch_id]                               BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

