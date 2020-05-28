CREATE TABLE [dbo].[stage_mms_EmployeeRole] (
    [stage_mms_EmployeeRole_id] BIGINT   NOT NULL,
    [EmployeeRoleID]            INT      NULL,
    [EmployeeID]                INT      NULL,
    [ValEmployeeRoleID]         INT      NULL,
    [InsertedDateTime]          DATETIME NULL,
    [UpdatedDateTime]           DATETIME NULL,
    [PrimaryEmployeeRoleFlag]   BIT      NULL,
    [dv_batch_id]               BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

