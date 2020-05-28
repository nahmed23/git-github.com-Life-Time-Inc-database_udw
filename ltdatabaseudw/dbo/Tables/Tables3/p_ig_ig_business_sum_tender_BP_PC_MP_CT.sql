CREATE TABLE [dbo].[p_ig_ig_business_sum_tender_BP_PC_MP_CT] (
    [p_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [tendered_business_period_dim_id]            INT          NULL,
    [posted_business_period_dim_id]              INT          NULL,
    [event_dim_id]                               INT          NULL,
    [profit_center_dim_id]                       INT          NULL,
    [meal_period_dim_id]                         INT          NULL,
    [check_type_dim_id]                          INT          NULL,
    [tender_dim_id]                              INT          NULL,
    [credit_type_id]                             INT          NULL,
    [s_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT       NULL,
    [dv_greatest_satellite_date_time]            DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]       DATETIME     NULL,
    [dv_load_date_time]                          DATETIME     NOT NULL,
    [dv_load_end_date_time]                      DATETIME     NOT NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL,
    [dv_first_in_key_series]                     BIT          NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ig_ig_business_sum_tender_BP_PC_MP_CT]
    ON [dbo].[p_ig_ig_business_sum_tender_BP_PC_MP_CT]([bk_hash] ASC, [p_ig_ig_business_sum_tender_BP_PC_MP_CT_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ig_ig_business_sum_tender_BP_PC_MP_CT]([dv_batch_id] ASC);

