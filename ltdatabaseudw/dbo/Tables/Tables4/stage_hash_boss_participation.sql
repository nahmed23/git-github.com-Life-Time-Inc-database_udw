CREATE TABLE [dbo].[stage_hash_boss_participation] (
    [stage_hash_boss_participation_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)    NOT NULL,
    [reservation]                      INT          NULL,
    [participation_date]               DATETIME     NULL,
    [no_participants]                  INT          NULL,
    [comment]                          VARCHAR (80) NULL,
    [no_non_mbr]                       INT          NULL,
    [updated_at]                       DATETIME     NULL,
    [created_at]                       DATETIME     NULL,
    [id]                               INT          NULL,
    [system_count]                     INT          NULL,
    [MOD_count]                        INT          NULL,
    [dv_load_date_time]                DATETIME     NOT NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

