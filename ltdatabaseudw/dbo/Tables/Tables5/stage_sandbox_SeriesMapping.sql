CREATE TABLE [dbo].[stage_sandbox_SeriesMapping] (
    [stage_sandbox_SeriesMapping_id] BIGINT        NOT NULL,
    [SeriesID]                       BIGINT        NULL,
    [Store_Number]                   INT           NULL,
    [SeriesName]                     VARCHAR (100) NULL,
    [Category]                       VARCHAR (25)  NULL,
    [Segment]                        VARCHAR (25)  NULL,
    [UpdatedDateTime]                DATETIME      NULL,
    [jan_one]                        DATETIME      NULL,
    [dv_batch_id]                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

