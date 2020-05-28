CREATE TABLE [dbo].[stage_mms_DepartmentUnit] (
    [stage_mms_DepartmentUnit_id] BIGINT        NOT NULL,
    [DepartmentUnitID]            INT           NULL,
    [DepartmentName]              VARCHAR (50)  NULL,
    [DepartmentHeadEmailAddress]  VARCHAR (140) NULL,
    [InsertedDateTime]            DATETIME      NULL,
    [UpdatedDateTime]             DATETIME      NULL,
    [DisplayUIFlag]               BIT           NULL,
    [dv_batch_id]                 BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

