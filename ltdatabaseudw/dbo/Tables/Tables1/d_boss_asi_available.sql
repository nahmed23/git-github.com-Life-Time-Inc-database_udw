CREATE TABLE [dbo].[d_boss_asi_available] (
    [d_boss_asi_available_id]              BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [club]                                 INT          NULL,
    [resource_id]                          INT          NULL,
    [start_time]                           DATETIME     NULL,
    [club_d_boss_asi_club_res_bk_hash]     CHAR (32)    NULL,
    [end_dim_date_key]                     CHAR (8)     NULL,
    [end_dim_time_key]                     CHAR (8)     NULL,
    [end_time]                             DATETIME     NULL,
    [resource_d_boss_asi_club_res_bk_hash] CHAR (32)    NULL,
    [schedule_type]                        CHAR (1)     NULL,
    [start_dim_date_key]                   CHAR (8)     NULL,
    [start_dim_time_key]                   CHAR (8)     NULL,
    [p_boss_asi_available_id]              BIGINT       NOT NULL,
    [deleted_flag]                         INT          NULL,
    [dv_load_date_time]                    DATETIME     NULL,
    [dv_load_end_date_time]                DATETIME     NULL,
    [dv_batch_id]                          BIGINT       NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

