CREATE TABLE [dbo].[stage_mms_Department] (
    [stage_mms_Department_id] BIGINT       NOT NULL,
    [DepartmentID]            INT          NULL,
    [Name]                    VARCHAR (15) NULL,
    [Description]             VARCHAR (50) NULL,
    [SortOrder]               CHAR (10)    NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

