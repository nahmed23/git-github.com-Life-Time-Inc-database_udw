CREATE TABLE [dbo].[h_ig_ig_business_sum_tips_BP_PC_MP_SE] (
    [h_ig_ig_business_sum_tips_BP_PC_MP_SE_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [tendered_business_period_dim_id]          INT          NULL,
    [posted_business_period_dim_id]            INT          NULL,
    [event_dim_id]                             INT          NULL,
    [profit_center_dim_id]                     INT          NULL,
    [meal_period_dim_id]                       INT          NULL,
    [server_emp_dim_id]                        INT          NULL,
    [dv_load_date_time]                        DATETIME     NOT NULL,
    [dv_r_load_source_id]                      BIGINT       NOT NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL,
    [dv_deleted]                               BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_ig_ig_business_sum_tips_BP_PC_MP_SE]
    ON [dbo].[h_ig_ig_business_sum_tips_BP_PC_MP_SE]([bk_hash] ASC, [h_ig_ig_business_sum_tips_BP_PC_MP_SE_id] ASC);

