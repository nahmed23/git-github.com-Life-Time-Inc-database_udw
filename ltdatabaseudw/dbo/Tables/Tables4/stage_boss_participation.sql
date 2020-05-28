CREATE TABLE [dbo].[stage_boss_participation] (
    [stage_boss_participation_id] BIGINT       NOT NULL,
    [reservation]                 INT          NULL,
    [participation_date]          DATETIME     NULL,
    [no_participants]             INT          NULL,
    [comment]                     VARCHAR (80) NULL,
    [no_non_mbr]                  INT          NULL,
    [updated_at]                  DATETIME     NULL,
    [created_at]                  DATETIME     NULL,
    [id]                          INT          NULL,
    [system_count]                INT          NULL,
    [MOD_count]                   INT          NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

