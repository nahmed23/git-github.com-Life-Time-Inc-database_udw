CREATE TABLE [dbo].[stage_mms_ChildCenterUsageException] (
    [stage_mms_ChildCenterUsageException_id] BIGINT   NOT NULL,
    [ChildCenterUsageExceptionID]            INT      NULL,
    [ChildCenterUsageID]                     INT      NULL,
    [EmployeeID]                             INT      NULL,
    [ValChildCenterUsageExceptionID]         SMALLINT NULL,
    [InsertedDatetime]                       DATETIME NULL,
    [UpdatedDateTime]                        DATETIME NULL,
    [dv_batch_id]                            BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

