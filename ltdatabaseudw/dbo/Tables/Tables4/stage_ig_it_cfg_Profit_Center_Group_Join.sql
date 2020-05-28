CREATE TABLE [dbo].[stage_ig_it_cfg_Profit_Center_Group_Join] (
    [stage_ig_it_cfg_Profit_Center_Group_Join_id] BIGINT   NOT NULL,
    [ent_id]                                      INT      NULL,
    [profit_ctr_grp_id]                           INT      NULL,
    [profit_center_id]                            INT      NULL,
    [jan_one]                                     DATETIME NULL,
    [dv_batch_id]                                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

