CREATE TABLE [dbo].[stage_mms_DeletedData] (
    [stage_mms_DeletedData_id] BIGINT        NOT NULL,
    [DeletedDataID]            INT           NULL,
    [TableName]                VARCHAR (100) NULL,
    [PrimaryKeyID]             INT           NULL,
    [DeletedDateTime]          DATETIME      NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

