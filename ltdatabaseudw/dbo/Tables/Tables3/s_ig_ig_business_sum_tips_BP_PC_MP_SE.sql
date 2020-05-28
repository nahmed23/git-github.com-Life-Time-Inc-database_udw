CREATE TABLE [dbo].[s_ig_ig_business_sum_tips_BP_PC_MP_SE] (
    [s_ig_ig_business_sum_tips_BP_PC_MP_SE_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [tendered_business_period_dim_id]          INT             NULL,
    [posted_business_period_dim_id]            INT             NULL,
    [event_dim_id]                             INT             NULL,
    [profit_center_dim_id]                     INT             NULL,
    [meal_period_dim_id]                       INT             NULL,
    [server_emp_dim_id]                        INT             NULL,
    [gross_sales_amount]                       DECIMAL (18, 4) NULL,
    [discount_amount]                          DECIMAL (18, 4) NULL,
    [irs_allocable_sales_amount]               DECIMAL (18, 4) NULL,
    [charged_tip_amount]                       DECIMAL (18, 4) NULL,
    [declared_cash_tip_amount]                 DECIMAL (18, 4) NULL,
    [charged_gratuity_amount]                  DECIMAL (18, 4) NULL,
    [tip_transfer_in_amount]                   DECIMAL (18, 4) NULL,
    [tip_transfer_out_amount]                  DECIMAL (18, 4) NULL,
    [charged_tip_grat_sales_amount]            DECIMAL (18, 4) NULL,
    [jan_one]                                  DATETIME        NULL,
    [dv_load_date_time]                        DATETIME        NOT NULL,
    [dv_r_load_source_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL,
    [dv_hash]                                  CHAR (32)       NOT NULL,
    [dv_batch_id]                              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_ig_business_sum_tips_BP_PC_MP_SE]
    ON [dbo].[s_ig_ig_business_sum_tips_BP_PC_MP_SE]([bk_hash] ASC, [s_ig_ig_business_sum_tips_BP_PC_MP_SE_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_ig_business_sum_tips_BP_PC_MP_SE]([dv_batch_id] ASC);

