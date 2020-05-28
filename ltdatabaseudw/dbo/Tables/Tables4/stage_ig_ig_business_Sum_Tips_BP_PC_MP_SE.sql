CREATE TABLE [dbo].[stage_ig_ig_business_Sum_Tips_BP_PC_MP_SE] (
    [stage_ig_ig_business_Sum_Tips_BP_PC_MP_SE_id] BIGINT          NOT NULL,
    [tendered_business_period_dim_id]              INT             NULL,
    [posted_business_period_dim_id]                INT             NULL,
    [event_dim_id]                                 INT             NULL,
    [profit_center_dim_id]                         INT             NULL,
    [meal_period_dim_id]                           INT             NULL,
    [server_emp_dim_id]                            INT             NULL,
    [gross_sales_amount]                           DECIMAL (18, 4) NULL,
    [discount_amount]                              DECIMAL (18, 4) NULL,
    [irs_allocable_sales_amount]                   DECIMAL (18, 4) NULL,
    [charged_tip_amount]                           DECIMAL (18, 4) NULL,
    [declared_cash_tip_amount]                     DECIMAL (18, 4) NULL,
    [charged_gratuity_amount]                      DECIMAL (18, 4) NULL,
    [tip_transfer_in_amount]                       DECIMAL (18, 4) NULL,
    [tip_transfer_out_amount]                      DECIMAL (18, 4) NULL,
    [charged_tip_grat_sales_amount]                DECIMAL (18, 4) NULL,
    [jan_one]                                      DATETIME        NULL,
    [dv_batch_id]                                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

