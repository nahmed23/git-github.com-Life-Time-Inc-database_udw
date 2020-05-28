CREATE TABLE [dbo].[stage_ltfeb_DeletedData] (
    [stage_ltfeb_DeletedData_id] BIGINT         NOT NULL,
    [DeletedDataID]              INT            NULL,
    [TableName]                  VARCHAR (100)  NULL,
    [PrimaryKey]                 NVARCHAR (200) NULL,
    [SecondPrimaryKey]           NVARCHAR (200) NULL,
    [DeletedDateTime]            DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

