CREATE TABLE [dbo].[l_boss_asi_player] (
    [l_boss_asi_player_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [reservation]           INT          NULL,
    [mbr_code]              CHAR (10)    NULL,
    [employee_id]           CHAR (10)    NULL,
    [asi_player_id]         INT          NULL,
    [contact_id]            INT          NULL,
    [mbrship_type_id]       INT          NULL,
    [mms_trans_id]          INT          NULL,
    [recurrence_id]         INT          NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_deleted]            BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_boss_asi_player]([dv_batch_id] ASC);

