﻿CREATE TABLE [dbo].[s_hybris_Ecommerce_payment_breakdown] (
    [s_hybris_Ecommerce_payment_breakdown_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [account_set]                             VARCHAR (255)   NULL,
    [line_company]                            VARCHAR (255)   NULL,
    [ledger_account]                          VARCHAR (255)   NULL,
    [workday_region]                          VARCHAR (4)     NULL,
    [cost_center]                             VARCHAR (255)   NULL,
    [spend_category]                          VARCHAR (255)   NULL,
    [revenue_category]                        VARCHAR (255)   NULL,
    [merchant_location]                       VARCHAR (255)   NULL,
    [order_num]                               VARCHAR (255)   NULL,
    [oe_num]                                  INT             NULL,
    [sub_cat_name]                            VARCHAR (255)   NULL,
    [product_name]                            VARCHAR (255)   NULL,
    [delivery_state]                          VARCHAR (255)   NULL,
    [tax]                                     DECIMAL (26, 6) NULL,
    [shipping]                                DECIMAL (26, 6) NULL,
    [ord_date]                                VARCHAR (29)    NULL,
    [tran_date]                               VARCHAR (29)    NULL,
    [ship_date]                               VARCHAR (29)    NULL,
    [capture_ltbucks]                         DECIMAL (10, 2) NULL,
    [capture_amex]                            DECIMAL (10, 2) NULL,
    [capture_discover]                        DECIMAL (10, 2) NULL,
    [capture_master]                          DECIMAL (10, 2) NULL,
    [capture_visa]                            DECIMAL (10, 2) NULL,
    [capture_paypal]                          DECIMAL (10, 2) NULL,
    [refund_ltbucks]                          DECIMAL (10, 2) NULL,
    [refund_amex]                             DECIMAL (10, 2) NULL,
    [refund_discover]                         DECIMAL (10, 2) NULL,
    [refund_master]                           DECIMAL (10, 2) NULL,
    [refund_visa]                             DECIMAL (10, 2) NULL,
    [refund_paypal]                           DECIMAL (10, 2) NULL,
    [report_start_date]                       DATETIME        NULL,
    [report_end_date]                         DATETIME        NULL,
    [jan_one]                                 DATETIME        NULL,
    [dv_load_date_time]                       DATETIME        NOT NULL,
    [dv_r_load_source_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL,
    [dv_hash]                                 CHAR (32)       NOT NULL,
    [dv_batch_id]                             BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_hybris_Ecommerce_payment_breakdown]([dv_batch_id] ASC);

