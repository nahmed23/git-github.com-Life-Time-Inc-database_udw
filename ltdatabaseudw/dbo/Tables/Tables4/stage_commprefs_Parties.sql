CREATE TABLE [dbo].[stage_commprefs_Parties] (
    [stage_commprefs_Parties_id] BIGINT   NOT NULL,
    [Id]                         INT      NULL,
    [CreatedTime]                DATETIME NULL,
    [UpdatedTime]                DATETIME NULL,
    [dv_batch_id]                BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

