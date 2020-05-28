CREATE TABLE [dbo].[stage_hash_boss_evt_registration_processes] (
    [stage_hash_boss_evt_registration_processes_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)     NOT NULL,
    [id]                                            INT           NULL,
    [member_id]                                     INT           NULL,
    [reservation_id]                                INT           NULL,
    [state]                                         VARCHAR (255) NULL,
    [created_at]                                    DATETIME      NULL,
    [updated_at]                                    DATETIME      NULL,
    [user_id]                                       INT           NULL,
    [roster_id]                                     INT           NULL,
    [expires_at]                                    DATETIME      NULL,
    [roster_only]                                   BIT           NULL,
    [dv_load_date_time]                             DATETIME      NOT NULL,
    [dv_inserted_date_time]                         DATETIME      NOT NULL,
    [dv_insert_user]                                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                          DATETIME      NULL,
    [dv_update_user]                                VARCHAR (50)  NULL,
    [dv_batch_id]                                   BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

