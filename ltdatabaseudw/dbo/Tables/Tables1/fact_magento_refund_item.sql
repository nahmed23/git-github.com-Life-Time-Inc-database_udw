﻿CREATE TABLE [dbo].[fact_magento_refund_item] (
    [fact_magento_refund_item_id]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [allocated_month_starting_dim_date_key]      VARCHAR (8)     NULL,
    [allocated_recalculate_through_datetime]     DATETIME        NULL,
    [allocated_recalculate_through_dim_date_key] VARCHAR (8)     NULL,
    [credit_memo_id]                             INT             NULL,
    [credit_memo_item_id]                        INT             NULL,
    [fact_magento_invoice_key]                   VARCHAR (32)    NULL,
    [fact_magento_order_item_key]                VARCHAR (32)    NULL,
    [fact_magento_payment_key]                   VARCHAR (32)    NULL,
    [fact_magento_refund_item_key]               VARCHAR (32)    NULL,
    [fact_magento_refund_key]                    VARCHAR (32)    NULL,
    [refund_adjustment_amount]                   DECIMAL (12, 2) NULL,
    [refund_currency_code]                       VARCHAR (3)     NULL,
    [refund_datetime]                            DATETIME        NULL,
    [refund_dim_date_key]                        VARCHAR (8)     NULL,
    [refund_item_amount]                         DECIMAL (12, 2) NULL,
    [refund_item_cost]                           DECIMAL (12, 2) NULL,
    [refund_item_discount_amount]                DECIMAL (12, 2) NULL,
    [refund_item_price]                          DECIMAL (12, 2) NULL,
    [refund_item_quantity]                       INT             NULL,
    [refund_item_tax_amount]                     DECIMAL (12, 2) NULL,
    [refund_reward_amount]                       DECIMAL (12, 2) NULL,
    [refund_shipping_amount]                     DECIMAL (12, 2) NULL,
    [refund_shipping_tax_amount]                 DECIMAL (12, 2) NULL,
    [refund_status]                              VARCHAR (50)    NULL,
    [dv_load_date_time]                          DATETIME        NULL,
    [dv_load_end_date_time]                      DATETIME        NULL,
    [dv_batch_id]                                BIGINT          NOT NULL,
    [dv_inserted_date_time]                      DATETIME        NOT NULL,
    [dv_insert_user]                             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                       DATETIME        NULL,
    [dv_update_user]                             VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_magento_refund_item_key]));
