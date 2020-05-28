CREATE TABLE [dbo].[stage_ig_ig_dimension_Business_Period_Dimension] (
    [stage_ig_ig_dimension_Business_Period_Dimension_id] BIGINT   NOT NULL,
    [business_period_dim_id]                             BIGINT   NULL,
    [customer_id]                                        INT      NULL,
    [ent_id]                                             INT      NULL,
    [business_period_group_id]                           INT      NULL,
    [start_date_time]                                    DATETIME NULL,
    [end_date_time]                                      DATETIME NULL,
    [dv_batch_id]                                        BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

