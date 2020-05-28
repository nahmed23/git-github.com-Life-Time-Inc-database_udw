CREATE TABLE [dbo].[d_chronotrack_event_location] (
    [d_chronotrack_event_location_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [event_location_id]               INT          NULL,
    [create_time]                     INT          NULL,
    [d_chronotrack_event_bk_hash]     VARCHAR (32) NULL,
    [d_chronotrack_location_bk_hash]  VARCHAR (32) NULL,
    [event_id]                        BIGINT       NULL,
    [location_id]                     BIGINT       NULL,
    [modified_time]                   INT          NULL,
    [p_chronotrack_event_location_id] BIGINT       NOT NULL,
    [deleted_flag]                    INT          NULL,
    [dv_load_date_time]               DATETIME     NULL,
    [dv_load_end_date_time]           DATETIME     NULL,
    [dv_batch_id]                     BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

