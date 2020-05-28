CREATE TABLE [dbo].[stage_mms_LTFKey] (
    [stage_mms_LTFKey_id] BIGINT        NOT NULL,
    [LTFKeyID]            INT           NULL,
    [Identifier]          NVARCHAR (50) NULL,
    [Name]                NVARCHAR (50) NULL,
    [InsertedDateTime]    DATETIME      NULL,
    [UpdatedDateTime]     DATETIME      NULL,
    [dv_batch_id]         BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

