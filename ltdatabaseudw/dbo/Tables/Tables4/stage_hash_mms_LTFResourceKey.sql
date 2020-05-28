CREATE TABLE [dbo].[stage_hash_mms_LTFResourceKey] (
    [stage_hash_mms_LTFResourceKey_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32) NOT NULL,
    [LTFResourceKeyID]                 INT       NULL,
    [LTFResourceID]                    INT       NULL,
    [LTFKeyID]                         INT       NULL,
    [InsertedDateTime]                 DATETIME  NULL,
    [UpdatedDateTime]                  DATETIME  NULL,
    [dv_load_date_time]                DATETIME  NOT NULL,
    [dv_batch_id]                      BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

