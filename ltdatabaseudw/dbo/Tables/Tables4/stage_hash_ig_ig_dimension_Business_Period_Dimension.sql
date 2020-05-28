CREATE TABLE [dbo].[stage_hash_ig_ig_dimension_Business_Period_Dimension] (
    [stage_hash_ig_ig_dimension_Business_Period_Dimension_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                 CHAR (32)    NOT NULL,
    [business_period_dim_id]                                  BIGINT       NULL,
    [customer_id]                                             INT          NULL,
    [ent_id]                                                  INT          NULL,
    [business_period_group_id]                                INT          NULL,
    [start_date_time]                                         DATETIME     NULL,
    [end_date_time]                                           DATETIME     NULL,
    [dv_load_date_time]                                       DATETIME     NOT NULL,
    [dv_inserted_date_time]                                   DATETIME     NOT NULL,
    [dv_insert_user]                                          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                                    DATETIME     NULL,
    [dv_update_user]                                          VARCHAR (50) NULL,
    [dv_batch_id]                                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

