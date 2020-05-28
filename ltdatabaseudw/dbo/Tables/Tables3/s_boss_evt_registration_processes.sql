CREATE TABLE [dbo].[s_boss_evt_registration_processes] (
    [s_boss_evt_registration_processes_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [evt_registration_processes_id]        INT           NULL,
    [state]                                VARCHAR (255) NULL,
    [created_at]                           DATETIME      NULL,
    [updated_at]                           DATETIME      NULL,
    [expires_at]                           DATETIME      NULL,
    [roster_only]                          BIT           NULL,
    [dv_load_date_time]                    DATETIME      NOT NULL,
    [dv_r_load_source_id]                  BIGINT        NOT NULL,
    [dv_inserted_date_time]                DATETIME      NOT NULL,
    [dv_insert_user]                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                 DATETIME      NULL,
    [dv_update_user]                       VARCHAR (50)  NULL,
    [dv_hash]                              CHAR (32)     NOT NULL,
    [dv_deleted]                           BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_evt_registration_processes]([dv_batch_id] ASC);

