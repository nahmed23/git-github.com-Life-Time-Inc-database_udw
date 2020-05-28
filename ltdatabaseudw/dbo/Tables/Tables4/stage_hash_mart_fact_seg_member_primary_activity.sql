CREATE TABLE [dbo].[stage_hash_mart_fact_seg_member_primary_activity] (
    [stage_hash_mart_fact_seg_member_primary_activity_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                             CHAR (32) NOT NULL,
    [fact_seg_member_primary_activity_id]                 INT       NULL,
    [member_id]                                           INT       NULL,
    [primary_activity_segment]                            INT       NULL,
    [row_add_date]                                        DATETIME  NULL,
    [active_flag]                                         INT       NULL,
    [row_deactivation_date]                               DATETIME  NULL,
    [confidence_score]                                    INT       NULL,
    [dv_load_date_time]                                   DATETIME  NOT NULL,
    [dv_batch_id]                                         BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

