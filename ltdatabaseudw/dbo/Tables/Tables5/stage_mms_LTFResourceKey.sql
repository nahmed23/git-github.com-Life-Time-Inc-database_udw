CREATE TABLE [dbo].[stage_mms_LTFResourceKey] (
    [stage_mms_LTFResourceKey_id] BIGINT   NOT NULL,
    [LTFResourceKeyID]            INT      NULL,
    [LTFResourceID]               INT      NULL,
    [LTFKeyID]                    INT      NULL,
    [InsertedDateTime]            DATETIME NULL,
    [UpdatedDateTime]             DATETIME NULL,
    [dv_batch_id]                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

