﻿CREATE TABLE [dbo].[fact_combined_allocated_transaction_item] (
    [sales_source]                           VARCHAR (14)    NOT NULL,
    [source_fact_table_key]                  VARCHAR (33)    NULL,
    [allocated_dim_club_key]                 VARCHAR (32)    NULL,
    [primary_sales_dim_employee_key]         VARCHAR (32)    NULL,
    [transaction_dim_date_key]               INT             NULL,
    [transaction_dim_time_key]               INT             NULL,
    [allocated_month_starting_dim_date_key]  VARCHAR (8)     NULL,
    [allocated_quantity]                     INT             NULL,
    [allocated_amount]                       DECIMAL (26, 6) NULL,
    [transaction_quantity]                   INT             NULL,
    [transaction_amount]                     DECIMAL (26, 6) NULL,
    [original_currency_code]                 VARCHAR (3)     NULL,
    [payment_types]                          VARCHAR (4000)  NULL,
    [discount_amount]                        DECIMAL (26, 6) NULL,
    [dim_mms_transaction_reason_key]         VARCHAR (32)    NULL,
    [transaction_type]                       VARCHAR (10)    NULL,
    [sales_channel_dim_description_key]      VARCHAR (32)    NULL,
    [transaction_id]                         VARCHAR (255)   NULL,
    [line_number]                            INT             NULL,
    [source_system]                          VARCHAR (500)   NULL,
    [dim_product_key]                        CHAR (32)       NULL,
    [source_product_id]                      VARCHAR (50)    NULL,
    [product_description]                    VARCHAR (500)   NULL,
    [dim_reporting_hierarchy_key]            CHAR (32)       NULL,
    [reporting_division]                     VARCHAR (500)   NULL,
    [reporting_sub_division]                 VARCHAR (500)   NULL,
    [reporting_department]                   VARCHAR (500)   NULL,
    [reporting_product_group]                VARCHAR (500)   NULL,
    [allocation_rule]                        VARCHAR (500)   NOT NULL,
    [ecommerce_shipment_number]              VARCHAR (255)   NULL,
    [ecommerce_order_number]                 INT             NULL,
    [ecommerce_autoship_flag]                VARCHAR (30)    NULL,
    [ecommerce_shipping_and_handling_amount] DECIMAL (12, 2) NULL,
    [ecommerce_product_cost]                 DECIMAL (26, 6) NULL,
    [ecommerce_deferral_flag]                VARCHAR (1)     NOT NULL,
    [mms_tran_id]                            INT             NULL,
    [mms_tran_item_id]                       INT             NULL,
    [exerp_sale_employee_id]                 INT             NULL,
    [exerp_service_employee_id]              INT             NULL,
    [dim_mms_membership_key]                 VARCHAR (32)    NULL,
    [membership_id]                          INT             NULL,
    [membership_type]                        VARCHAR (50)    NULL,
    [dim_mms_member_key]                     VARCHAR (32)    NULL,
    [member_id]                              INT             NULL,
    [member_name]                            VARCHAR (102)   NULL,
    [member_first_name]                      VARCHAR (50)    NULL,
    [member_last_name]                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([source_fact_table_key]));

