CREATE TABLE [dbo].[d_mart_seg_member_usage_group_history] (
    [d_mart_seg_member_usage_group_history_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [dim_mart_seg_member_usage_group_key]      VARCHAR (32) NULL,
    [usage_group_segment_id]                   INT          NULL,
    [effective_date_time]                      DATETIME     NULL,
    [expiration_date_time]                     DATETIME     NULL,
    [active_flag]                              CHAR (1)     NULL,
    [max_swipes_week]                          INT          NULL,
    [min_swipes_week]                          INT          NULL,
    [usage_group]                              CHAR (20)    NULL,
    [p_mart_seg_member_usage_group_id]         BIGINT       NOT NULL,
    [deleted_flag]                             INT          NULL,
    [dv_load_date_time]                        DATETIME     NULL,
    [dv_load_end_date_time]                    DATETIME     NULL,
    [dv_batch_id]                              BIGINT       NOT NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

