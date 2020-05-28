CREATE TABLE [dbo].[stage_mms_ValEmployeeRole] (
    [stage_mms_ValEmployeeRole_id] BIGINT       NOT NULL,
    [ValEmployeeRoleID]            INT          NULL,
    [LTUPositionID]                INT          NULL,
    [Description]                  VARCHAR (50) NULL,
    [SortOrder]                    INT          NULL,
    [DepartmentID]                 INT          NULL,
    [CommissionableFlag]           BIT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [HRJobCode]                    VARCHAR (8)  NULL,
    [CompanyInsiderType]           VARCHAR (50) NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

