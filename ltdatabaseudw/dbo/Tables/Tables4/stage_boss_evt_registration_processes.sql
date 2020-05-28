CREATE TABLE [dbo].[stage_boss_evt_registration_processes] (
    [stage_boss_evt_registration_processes_id] BIGINT        NOT NULL,
    [id]                                       INT           NULL,
    [member_id]                                INT           NULL,
    [reservation_id]                           INT           NULL,
    [state]                                    VARCHAR (255) NULL,
    [created_at]                               DATETIME      NULL,
    [updated_at]                               DATETIME      NULL,
    [user_id]                                  INT           NULL,
    [roster_id]                                INT           NULL,
    [expires_at]                               DATETIME      NULL,
    [roster_only]                              BIT           NULL,
    [dv_batch_id]                              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

