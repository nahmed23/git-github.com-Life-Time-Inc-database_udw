﻿CREATE TABLE [dbo].[d_hybris_seven_day_orders] (
    [d_hybris_seven_day_orders_id]                      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [order_code]                                        NVARCHAR (255)  NULL,
    [entry_number]                                      INT             NULL,
    [affiliate_id]                                      NVARCHAR (255)  NULL,
    [auto_ship_flag]                                    CHAR (1)        NULL,
    [base_refund_amount]                                DECIMAL (8, 2)  NULL,
    [capture_amex]                                      DECIMAL (26, 6) NULL,
    [capture_discover]                                  DECIMAL (26, 6) NULL,
    [capture_lt_bucks]                                  DECIMAL (26, 6) NULL,
    [capture_master]                                    DECIMAL (26, 6) NULL,
    [capture_paypal]                                    DECIMAL (26, 6) NULL,
    [capture_visa]                                      DECIMAL (26, 6) NULL,
    [commission_employee_id]                            NVARCHAR (255)  NULL,
    [customer_email]                                    NVARCHAR (255)  NULL,
    [customer_group]                                    NVARCHAR (255)  NULL,
    [customer_name]                                     NVARCHAR (255)  NULL,
    [dim_hybris_product_key]                            VARCHAR (32)    NULL,
    [discount_amount]                                   DECIMAL (26, 6) NULL,
    [fact_mms_sales_transaction_key]                    VARCHAR (32)    NULL,
    [fulfillment_partner]                               NVARCHAR (255)  NULL,
    [lt_bucks_earned]                                   DECIMAL (26, 6) NULL,
    [ltf_party_id]                                      NVARCHAR (255)  NULL,
    [method_of_pay]                                     NVARCHAR (255)  NULL,
    [mms_transaction_id]                                INT             NULL,
    [order_datetime]                                    DATETIME        NULL,
    [order_dim_date_key]                                VARCHAR (8)     NULL,
    [original_unit_price]                               DECIMAL (26, 6) NULL,
    [product_code]                                      NVARCHAR (258)  NULL,
    [purchase_unit_price]                               DECIMAL (26, 6) NULL,
    [refund_allocated_month_starting_dim_date_key]      VARCHAR (8)     NULL,
    [refund_allocated_recalculate_through_datetime]     DATETIME        NULL,
    [refund_allocated_recalculate_through_dim_date_key] VARCHAR (8)     NULL,
    [refund_amex]                                       DECIMAL (26, 6) NULL,
    [refund_amount]                                     DECIMAL (26, 6) NULL,
    [refund_amount_gross]                               DECIMAL (26, 6) NULL,
    [refund_datetime]                                   DATETIME        NULL,
    [refund_discover]                                   DECIMAL (26, 6) NULL,
    [refund_fact_hybris_transaction_item_key]           VARCHAR (32)    NULL,
    [refund_flag]                                       CHAR (1)        NULL,
    [refund_lt_bucks]                                   DECIMAL (26, 6) NULL,
    [refund_master]                                     DECIMAL (26, 6) NULL,
    [refund_paypal]                                     DECIMAL (26, 6) NULL,
    [refund_quantity]                                   INT             NULL,
    [refund_reason]                                     NVARCHAR (255)  NULL,
    [refund_shipping_and_handling_amount]               DECIMAL (26, 6) NULL,
    [refund_status]                                     NVARCHAR (255)  NULL,
    [refund_tax_amount]                                 DECIMAL (26, 6) NULL,
    [refund_visa]                                       DECIMAL (26, 6) NULL,
    [sale_allocated_month_starting_dim_date_key]        VARCHAR (8)     NULL,
    [sale_allocated_recalculate_through_datetime]       DATETIME        NULL,
    [sale_allocated_recalculate_through_dim_date_key]   VARCHAR (8)     NULL,
    [sale_fact_hybris_transaction_item_key]             VARCHAR (32)    NULL,
    [sales_amount]                                      DECIMAL (26, 6) NULL,
    [sales_amount_gross]                                DECIMAL (26, 6) NULL,
    [sales_dim_employee_key]                            VARCHAR (32)    NULL,
    [sales_quantity]                                    INT             NULL,
    [sales_shipping_and_handling_amount]                DECIMAL (26, 6) NULL,
    [sales_tax_amount]                                  DECIMAL (26, 6) NULL,
    [selected_club_id]                                  INT             NULL,
    [selected_dim_club_key]                             VARCHAR (32)    NULL,
    [settlement_datetime]                               DATETIME        NULL,
    [settlement_dim_date_key]                           VARCHAR (8)     NULL,
    [settlement_dim_time_key]                           INT             NULL,
    [tracking_number]                                   NVARCHAR (255)  NULL,
    [p_hybris_seven_day_orders_id]                      BIGINT          NOT NULL,
    [deleted_flag]                                      INT             NULL,
    [dv_load_date_time]                                 DATETIME        NULL,
    [dv_load_end_date_time]                             DATETIME        NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));
