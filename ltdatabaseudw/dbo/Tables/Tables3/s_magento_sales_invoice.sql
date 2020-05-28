﻿CREATE TABLE [dbo].[s_magento_sales_invoice] (
    [s_magento_sales_invoice_id]                   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)       NOT NULL,
    [entity_id]                                    INT             NULL,
    [base_grand_total]                             DECIMAL (12, 4) NULL,
    [shipping_tax_amount]                          DECIMAL (12, 4) NULL,
    [tax_amount]                                   DECIMAL (12, 4) NULL,
    [base_tax_amount]                              DECIMAL (12, 4) NULL,
    [store_to_order_rate]                          DECIMAL (12, 4) NULL,
    [base_shipping_tax_amount]                     DECIMAL (12, 4) NULL,
    [base_discount_amount]                         DECIMAL (12, 4) NULL,
    [base_to_order_rate]                           DECIMAL (12, 4) NULL,
    [grand_total]                                  DECIMAL (12, 4) NULL,
    [shipping_amount]                              DECIMAL (12, 4) NULL,
    [subtotal_incl_tax]                            DECIMAL (12, 4) NULL,
    [base_subtotal_incl_tax]                       DECIMAL (12, 4) NULL,
    [store_to_base_rate]                           DECIMAL (12, 4) NULL,
    [base_shipping_amount]                         DECIMAL (12, 4) NULL,
    [total_qty]                                    DECIMAL (12, 4) NULL,
    [base_to_global_rate]                          DECIMAL (12, 4) NULL,
    [subtotal]                                     DECIMAL (12, 4) NULL,
    [base_subtotal]                                DECIMAL (12, 4) NULL,
    [discount_amount]                              DECIMAL (12, 4) NULL,
    [is_used_for_refund]                           INT             NULL,
    [email_sent]                                   INT             NULL,
    [send_email]                                   INT             NULL,
    [can_void_flag]                                INT             NULL,
    [state]                                        INT             NULL,
    [store_currency_code]                          VARCHAR (3)     NULL,
    [transaction_id]                               VARCHAR (255)   NULL,
    [order_currency_code]                          VARCHAR (3)     NULL,
    [base_currency_code]                           VARCHAR (3)     NULL,
    [global_currency_code]                         VARCHAR (3)     NULL,
    [created_at]                                   DATETIME        NULL,
    [updated_at]                                   DATETIME        NULL,
    [discount_tax_compensation_amount]             DECIMAL (12, 4) NULL,
    [base_discount_tax_compensation_amount]        DECIMAL (12, 4) NULL,
    [shipping_discount_tax_compensation_amount]    DECIMAL (12, 4) NULL,
    [base_shipping_discount_tax_compensation_amnt] DECIMAL (12, 4) NULL,
    [shipping_incl_tax]                            DECIMAL (12, 4) NULL,
    [base_shipping_incl_tax]                       DECIMAL (12, 4) NULL,
    [base_total_refunded]                          DECIMAL (12, 4) NULL,
    [discount_description]                         VARCHAR (255)   NULL,
    [customer_note]                                VARCHAR (8000)  NULL,
    [customer_note_notify]                         INT             NULL,
    [base_customer_balance_amount]                 DECIMAL (12, 4) NULL,
    [customer_balance_amount]                      DECIMAL (12, 4) NULL,
    [base_gift_cards_amount]                       DECIMAL (12, 4) NULL,
    [gift_cards_amount]                            DECIMAL (12, 4) NULL,
    [gw_base_price]                                DECIMAL (12, 4) NULL,
    [gw_price]                                     DECIMAL (12, 4) NULL,
    [gw_items_base_price]                          DECIMAL (12, 4) NULL,
    [gw_items_price]                               DECIMAL (12, 4) NULL,
    [gw_card_base_price]                           DECIMAL (12, 4) NULL,
    [gw_card_price]                                DECIMAL (12, 4) NULL,
    [gw_base_tax_amount]                           DECIMAL (12, 4) NULL,
    [gw_tax_amount]                                DECIMAL (12, 4) NULL,
    [gw_items_base_tax_amount]                     DECIMAL (12, 4) NULL,
    [gw_items_tax_amount]                          DECIMAL (12, 4) NULL,
    [gw_card_base_tax_amount]                      DECIMAL (12, 4) NULL,
    [gw_card_tax_amount]                           DECIMAL (12, 4) NULL,
    [base_reward_currency_amount]                  DECIMAL (12, 4) NULL,
    [reward_currency_amount]                       DECIMAL (12, 4) NULL,
    [reward_points_balance]                        INT             NULL,
    [dv_load_date_time]                            DATETIME        NOT NULL,
    [dv_r_load_source_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                        DATETIME        NOT NULL,
    [dv_insert_user]                               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                         DATETIME        NULL,
    [dv_update_user]                               VARCHAR (50)    NULL,
    [dv_hash]                                      CHAR (32)       NOT NULL,
    [dv_deleted]                                   BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                  BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

