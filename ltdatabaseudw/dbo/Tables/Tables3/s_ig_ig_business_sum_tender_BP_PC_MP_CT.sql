CREATE TABLE [dbo].[s_ig_ig_business_sum_tender_BP_PC_MP_CT] (
    [s_ig_ig_business_sum_tender_BP_PC_MP_CT_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)       NOT NULL,
    [tendered_business_period_dim_id]            INT             NULL,
    [posted_business_period_dim_id]              INT             NULL,
    [event_dim_id]                               INT             NULL,
    [profit_center_dim_id]                       INT             NULL,
    [meal_period_dim_id]                         INT             NULL,
    [check_type_dim_id]                          INT             NULL,
    [tender_dim_id]                              INT             NULL,
    [credit_type_id]                             INT             NULL,
    [tender_amount]                              DECIMAL (18, 4) NULL,
    [change_amount]                              DECIMAL (18, 4) NULL,
    [received_amount]                            DECIMAL (18, 4) NULL,
    [breakage_amount]                            DECIMAL (18, 4) NULL,
    [tip_amount]                                 DECIMAL (18, 4) NULL,
    [tender_count]                               INT             NULL,
    [tender_quantity]                            INT             NULL,
    [jan_one]                                    DATETIME        NULL,
    [dv_load_date_time]                          DATETIME        NOT NULL,
    [dv_batch_id]                                BIGINT          NOT NULL,
    [dv_r_load_source_id]                        BIGINT          NOT NULL,
    [dv_inserted_date_time]                      DATETIME        NOT NULL,
    [dv_insert_user]                             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                       DATETIME        NULL,
    [dv_update_user]                             VARCHAR (50)    NULL,
    [dv_hash]                                    CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_ig_business_sum_tender_BP_PC_MP_CT]
    ON [dbo].[s_ig_ig_business_sum_tender_BP_PC_MP_CT]([bk_hash] ASC, [s_ig_ig_business_sum_tender_BP_PC_MP_CT_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_ig_business_sum_tender_BP_PC_MP_CT]([dv_batch_id] ASC);

