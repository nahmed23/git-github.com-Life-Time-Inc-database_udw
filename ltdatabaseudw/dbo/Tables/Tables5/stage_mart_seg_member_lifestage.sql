CREATE TABLE [dbo].[stage_mart_seg_member_lifestage] (
    [stage_mart_seg_member_lifestage_id] BIGINT        NOT NULL,
    [lifestage_segment_id]               INT           NULL,
    [lifestage_description]              VARCHAR (255) NULL,
    [gender]                             VARCHAR (3)   NULL,
    [has_kids]                           INT           NULL,
    [min_age]                            INT           NULL,
    [max_age]                            INT           NULL,
    [dummy_modified_date_time]           DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

