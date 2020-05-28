CREATE TABLE [dbo].[d_ig_ig_business_sum_tender_BP_PC_MP_CT] (
    [d_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)       NOT NULL,
    [fact_cafe_tender_by_check_type_summary_key] CHAR (32)       NULL,
    [event_dim_id]                               INT             NULL,
    [meal_period_dim_id]                         INT             NULL,
    [check_type_dim_id]                          INT             NULL,
    [credit_type_id]                             INT             NULL,
    [new_change_amount]                          DECIMAL (18, 4) NULL,
    [new_tender_amount]                          DECIMAL (18, 4) NULL,
    [posted_business_period_dim_id]              INT             NULL,
    [profit_center_dim_id]                       INT             NULL,
    [tender_dim_id]                              INT             NULL,
    [tender_net_amount]                          DECIMAL (18, 4) NULL,
    [tendered_business_period_dim_id]            INT             NULL,
    [p_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT          NOT NULL,
    [dv_load_date_time]                          DATETIME        NULL,
    [dv_load_end_date_time]                      DATETIME        NULL,
    [dv_batch_id]                                BIGINT          NOT NULL,
    [dv_inserted_date_time]                      DATETIME        NOT NULL,
    [dv_insert_user]                             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                       DATETIME        NULL,
    [dv_update_user]                             VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_ig_business_sum_tender_BP_PC_MP_CT]([dv_batch_id] ASC);

