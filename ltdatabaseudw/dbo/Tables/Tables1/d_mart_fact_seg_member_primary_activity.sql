CREATE TABLE [dbo].[d_mart_fact_seg_member_primary_activity] (
    [d_mart_fact_seg_member_primary_activity_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [fact_seg_member_primary_activity_key]       VARCHAR (32) NULL,
    [fact_seg_member_primary_activity_id]        INT          NULL,
    [active_flag]                                CHAR (1)     NULL,
    [confidence_score]                           INT          NULL,
    [dim_mms_member_key]                         VARCHAR (32) NULL,
    [member_id]                                  INT          NULL,
    [primary_activity_segment]                   INT          NULL,
    [row_add_date]                               DATETIME     NULL,
    [row_add_dim_date_key]                       VARCHAR (8)  NULL,
    [row_add_dim_time_key]                       INT          NULL,
    [row_deactivation_date]                      DATETIME     NULL,
    [row_deactivation_dim_date_key]              VARCHAR (8)  NULL,
    [row_deactivation_dim_time_key]              INT          NULL,
    [p_mart_fact_seg_member_primary_activity_id] BIGINT       NOT NULL,
    [deleted_flag]                               INT          NULL,
    [dv_load_date_time]                          DATETIME     NULL,
    [dv_load_end_date_time]                      DATETIME     NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

