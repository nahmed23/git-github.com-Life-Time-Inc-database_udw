CREATE TABLE [dbo].[p_udwcloudsync_club_master_roster] (
    [p_udwcloudsync_club_master_roster_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [mms_club_id]                          INT          NULL,
    [l_udwcloudsync_club_master_roster_id] BIGINT       NULL,
    [s_udwcloudsync_club_master_roster_id] BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_udwcloudsync_club_master_roster]
    ON [dbo].[p_udwcloudsync_club_master_roster]([bk_hash] ASC, [p_udwcloudsync_club_master_roster_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_udwcloudsync_club_master_roster]([dv_batch_id] ASC);

