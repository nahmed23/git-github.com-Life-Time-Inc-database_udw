CREATE TABLE [dbo].[d_mms_kids_play_check_in] (
    [d_mms_kids_play_check_in_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [fact_mms_kids_play_check_in_key]   CHAR (32)    NULL,
    [kids_play_check_in_id]             INT          NULL,
    [check_in_dim_date_key]             CHAR (8)     NULL,
    [check_in_dim_time_key]             CHAR (8)     NULL,
    [child_center_usage_id]             INT          NULL,
    [kids_play_check_in_date_time]      DATETIME     NULL,
    [kids_play_check_in_date_time_zone] CHAR (4)     NULL,
    [utc_kids_play_check_in_date_time]  DATETIME     NULL,
    [p_mms_kids_play_check_in_id]       BIGINT       NOT NULL,
    [dv_load_date_time]                 DATETIME     NULL,
    [dv_load_end_date_time]             DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_kids_play_check_in]([dv_batch_id] ASC);

