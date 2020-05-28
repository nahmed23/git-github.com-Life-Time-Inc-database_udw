CREATE TABLE [dbo].[h_ig_ig_business_sum_tender_BP_PC_MP_CT] (
    [h_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [tendered_business_period_dim_id]            INT          NULL,
    [posted_business_period_dim_id]              INT          NULL,
    [event_dim_id]                               INT          NULL,
    [profit_center_dim_id]                       INT          NULL,
    [meal_period_dim_id]                         INT          NULL,
    [check_type_dim_id]                          INT          NULL,
    [tender_dim_id]                              INT          NULL,
    [credit_type_id]                             INT          NULL,
    [dv_load_date_time]                          DATETIME     NOT NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_r_load_source_id]                        BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL,
    [dv_deleted]                                 BIT          DEFAULT ((0)) NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_h_ig_ig_business_sum_tender_BP_PC_MP_CT]
    ON [dbo].[h_ig_ig_business_sum_tender_BP_PC_MP_CT]([bk_hash] ASC, [h_ig_ig_business_sum_tender_BP_PC_MP_CT_id] ASC);

