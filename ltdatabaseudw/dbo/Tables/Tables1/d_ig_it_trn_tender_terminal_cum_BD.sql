﻿CREATE TABLE [dbo].[d_ig_it_trn_tender_terminal_cum_BD] (
    [d_ig_it_trn_tender_terminal_cum_BD_id]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [fact_cafe_tender_terminal_cumulative_key] VARCHAR (32)    NULL,
    [bus_day_id]                               INT             NULL,
    [check_type_id]                            INT             NULL,
    [meal_period_id]                           INT             NULL,
    [profit_center_id]                         INT             NULL,
    [tender_id]                                INT             NULL,
    [term_id]                                  INT             NULL,
    [void_type_id]                             INT             NULL,
    [base_tender_amount]                       DECIMAL (26, 6) NULL,
    [breakage_amount]                          DECIMAL (26, 6) NULL,
    [business_day_end_date_time]               DATETIME        NULL,
    [business_day_end_dim_date_key]            VARCHAR (8)     NULL,
    [business_day_end_dim_time_key]            INT             NULL,
    [business_day_start_date_time]             DATETIME        NULL,
    [business_day_start_dim_date_key]          VARCHAR (8)     NULL,
    [business_day_start_dim_time_key]          INT             NULL,
    [cash_drop_amount]                         DECIMAL (26, 6) NULL,
    [change_amount]                            DECIMAL (26, 6) NULL,
    [dim_cafe_business_day_dates_key]          VARCHAR (32)    NULL,
    [dim_cafe_check_type_key]                  VARCHAR (32)    NULL,
    [dim_cafe_meal_period_key]                 VARCHAR (32)    NULL,
    [dim_cafe_payment_type_key]                VARCHAR (32)    NULL,
    [dim_cafe_profit_center_key]               VARCHAR (32)    NULL,
    [dim_cafe_terminal_key]                    VARCHAR (32)    NULL,
    [loan_amount]                              DECIMAL (26, 6) NULL,
    [paid_out_amount]                          DECIMAL (26, 6) NULL,
    [received_current_amount]                  DECIMAL (26, 6) NULL,
    [tender_quantity]                          INT             NULL,
    [tip_amount]                               DECIMAL (26, 6) NULL,
    [withdrawal_amount]                        DECIMAL (26, 6) NULL,
    [p_ig_it_trn_tender_terminal_cum_BD_id]    BIGINT          NOT NULL,
    [deleted_flag]                             INT             NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

