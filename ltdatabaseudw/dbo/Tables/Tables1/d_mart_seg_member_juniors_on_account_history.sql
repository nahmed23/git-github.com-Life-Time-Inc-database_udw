CREATE TABLE [dbo].[d_mart_seg_member_juniors_on_account_history] (
    [d_mart_seg_member_juniors_on_account_history_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)    NOT NULL,
    [dim_juniors_on_account_segment_key]              VARCHAR (32) NULL,
    [juniors_on_account_segment_id]                   INT          NULL,
    [effective_date_time]                             DATETIME     NULL,
    [expiration_date_time]                            DATETIME     NULL,
    [active_flag]                                     CHAR (1)     NULL,
    [juniors_on_account]                              CHAR (255)   NULL,
    [p_mart_seg_member_juniors_on_account_id]         BIGINT       NOT NULL,
    [deleted_flag]                                    INT          NULL,
    [dv_load_date_time]                               DATETIME     NULL,
    [dv_load_end_date_time]                           DATETIME     NULL,
    [dv_batch_id]                                     BIGINT       NOT NULL,
    [dv_inserted_date_time]                           DATETIME     NOT NULL,
    [dv_insert_user]                                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                            DATETIME     NULL,
    [dv_update_user]                                  VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

