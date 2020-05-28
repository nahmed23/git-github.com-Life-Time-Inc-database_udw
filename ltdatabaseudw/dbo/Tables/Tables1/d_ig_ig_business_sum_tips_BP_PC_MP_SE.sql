CREATE TABLE [dbo].[d_ig_ig_business_sum_tips_BP_PC_MP_SE] (
    [d_ig_ig_business_sum_tips_BP_PC_MP_SE_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)       NOT NULL,
    [dim_ig_ig_business_sum_tips_bp_pc_mp_se_key] CHAR (32)       NULL,
    [tendered_business_period_dim_id]             INT             NULL,
    [posted_business_period_dim_id]               INT             NULL,
    [event_dim_id]                                INT             NULL,
    [profit_center_dim_id]                        INT             NULL,
    [meal_period_dim_id]                          INT             NULL,
    [server_emp_dim_id]                           INT             NULL,
    [charged_gratuity_amount]                     DECIMAL (18, 4) NULL,
    [charged_tip_amount]                          DECIMAL (18, 4) NULL,
    [p_ig_ig_business_sum_tips_BP_PC_MP_SE_id]    BIGINT          NOT NULL,
    [deleted_flag]                                INT             NULL,
    [dv_load_date_time]                           DATETIME        NULL,
    [dv_load_end_date_time]                       DATETIME        NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL,
    [dv_inserted_date_time]                       DATETIME        NOT NULL,
    [dv_insert_user]                              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                        DATETIME        NULL,
    [dv_update_user]                              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_ig_business_sum_tips_BP_PC_MP_SE]([dv_batch_id] ASC);

