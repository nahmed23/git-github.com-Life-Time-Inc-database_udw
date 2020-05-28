CREATE TABLE [dbo].[p_ig_ig_business_sum_tips_BP_PC_MP_SE] (
    [p_ig_ig_business_sum_tips_BP_PC_MP_SE_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [tendered_business_period_dim_id]          INT          NULL,
    [posted_business_period_dim_id]            INT          NULL,
    [event_dim_id]                             INT          NULL,
    [profit_center_dim_id]                     INT          NULL,
    [meal_period_dim_id]                       INT          NULL,
    [server_emp_dim_id]                        INT          NULL,
    [s_ig_ig_business_sum_tips_BP_PC_MP_SE_id] BIGINT       NULL,
    [dv_load_date_time]                        DATETIME     NOT NULL,
    [dv_load_end_date_time]                    DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]          DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]     DATETIME     NULL,
    [dv_first_in_key_series]                   INT          NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL,
    [dv_batch_id]                              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ig_ig_business_sum_tips_BP_PC_MP_SE]
    ON [dbo].[p_ig_ig_business_sum_tips_BP_PC_MP_SE]([bk_hash] ASC, [p_ig_ig_business_sum_tips_BP_PC_MP_SE_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ig_ig_business_sum_tips_BP_PC_MP_SE]([dv_batch_id] ASC);

