CREATE TABLE [dbo].[d_mart_seg_member_lifestage_history] (
    [d_mart_seg_member_lifestage_history_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)     NOT NULL,
    [dim_mart_seg_member_lifestage_key]      VARCHAR (32)  NULL,
    [lifestage_segment_id]                   INT           NULL,
    [effective_date_time]                    DATETIME      NULL,
    [expiration_date_time]                   DATETIME      NULL,
    [active_flag]                            CHAR (1)      NULL,
    [gender]                                 VARCHAR (3)   NULL,
    [has_kids]                               INT           NULL,
    [lifestage_description]                  VARCHAR (255) NULL,
    [max_age]                                INT           NULL,
    [min_age]                                INT           NULL,
    [p_mart_seg_member_lifestage_id]         BIGINT        NOT NULL,
    [deleted_flag]                           INT           NULL,
    [dv_load_date_time]                      DATETIME      NULL,
    [dv_load_end_date_time]                  DATETIME      NULL,
    [dv_batch_id]                            BIGINT        NOT NULL,
    [dv_inserted_date_time]                  DATETIME      NOT NULL,
    [dv_insert_user]                         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                   DATETIME      NULL,
    [dv_update_user]                         VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

