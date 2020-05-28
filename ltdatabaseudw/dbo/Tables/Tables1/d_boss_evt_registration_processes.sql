CREATE TABLE [dbo].[d_boss_evt_registration_processes] (
    [d_boss_evt_registration_processes_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [evt_registration_processes_id]        INT           NULL,
    [created_dim_date_key]                 CHAR (8)      NULL,
    [created_dim_time_key]                 CHAR (8)      NULL,
    [d_boss_asi_reserv_bk_hash]            CHAR (32)     NULL,
    [d_lt_bucks_users_bk_hash]             CHAR (32)     NULL,
    [d_mms_member_bk_hash]                 CHAR (32)     NULL,
    [evt_registration_processes_state]     VARCHAR (255) NULL,
    [expires_dim_date_key]                 CHAR (8)      NULL,
    [expires_dim_time_key]                 CHAR (8)      NULL,
    [member_id]                            INT           NULL,
    [reservation_id]                       INT           NULL,
    [roster_flag]                          CHAR (1)      NULL,
    [roster_id]                            INT           NULL,
    [updated_dim_date_key]                 CHAR (8)      NULL,
    [updated_dim_time_key]                 CHAR (8)      NULL,
    [user_id]                              INT           NULL,
    [p_boss_evt_registration_processes_id] BIGINT        NOT NULL,
    [deleted_flag]                         INT           NULL,
    [dv_load_date_time]                    DATETIME      NULL,
    [dv_load_end_date_time]                DATETIME      NULL,
    [dv_batch_id]                          BIGINT        NOT NULL,
    [dv_inserted_date_time]                DATETIME      NOT NULL,
    [dv_insert_user]                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                 DATETIME      NULL,
    [dv_update_user]                       VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_evt_registration_processes]([dv_batch_id] ASC);

