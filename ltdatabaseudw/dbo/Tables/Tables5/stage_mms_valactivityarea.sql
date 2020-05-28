CREATE TABLE [dbo].[stage_mms_valactivityarea] (
    [stage_mms_valactivityarea_id] BIGINT       NOT NULL,
    [valactivityareaid]            BIGINT       NULL,
    [description]                  VARCHAR (50) NULL,
    [sortorder]                    BIGINT       NULL,
    [inserteddatetime]             DATETIME     NULL,
    [updateddatetime]              DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

