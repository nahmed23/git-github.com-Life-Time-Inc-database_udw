CREATE TABLE [dbo].[stage_ig_ig_business_Sum_Tender_BP_PC_MP_CT] (
    [stage_ig_ig_business_Sum_Tender_BP_PC_MP_CT_id] BIGINT          NOT NULL,
    [tendered_business_period_dim_id]                INT             NULL,
    [posted_business_period_dim_id]                  INT             NULL,
    [event_dim_id]                                   INT             NULL,
    [profit_center_dim_id]                           INT             NULL,
    [meal_period_dim_id]                             INT             NULL,
    [check_type_dim_id]                              INT             NULL,
    [tender_dim_id]                                  INT             NULL,
    [credit_type_id]                                 INT             NULL,
    [tender_amount]                                  DECIMAL (18, 4) NULL,
    [change_amount]                                  DECIMAL (18, 4) NULL,
    [received_amount]                                DECIMAL (18, 4) NULL,
    [breakage_amount]                                DECIMAL (18, 4) NULL,
    [tip_amount]                                     DECIMAL (18, 4) NULL,
    [tender_count]                                   INT             NULL,
    [tender_quantity]                                INT             NULL,
    [jan_one]                                        DATETIME        NULL,
    [dv_batch_id]                                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

