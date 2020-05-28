CREATE TABLE [dbo].[stage_hash_mms_LTFKey] (
    [stage_hash_mms_LTFKey_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)     NOT NULL,
    [LTFKeyID]                 INT           NULL,
    [Identifier]               NVARCHAR (50) NULL,
    [Name]                     NVARCHAR (50) NULL,
    [InsertedDateTime]         DATETIME      NULL,
    [UpdatedDateTime]          DATETIME      NULL,
    [dv_load_date_time]        DATETIME      NOT NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

