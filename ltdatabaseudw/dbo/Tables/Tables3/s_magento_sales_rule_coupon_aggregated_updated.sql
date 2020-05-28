﻿CREATE TABLE [dbo].[s_magento_sales_rule_coupon_aggregated_updated] (
    [s_magento_sales_rule_coupon_aggregated_updated_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [id]                                                INT             NULL,
    [period]                                            DATE            NULL,
    [order_status]                                      VARCHAR (50)    NULL,
    [coupon_code]                                       VARCHAR (50)    NULL,
    [coupon_uses]                                       INT             NULL,
    [subtotal_amount]                                   DECIMAL (12, 4) NULL,
    [discount_amount]                                   DECIMAL (12, 4) NULL,
    [total_amount]                                      DECIMAL (12, 4) NULL,
    [subtotal_amount_actual]                            DECIMAL (12, 4) NULL,
    [discount_amount_actual]                            DECIMAL (12, 4) NULL,
    [total_amount_actual]                               DECIMAL (12, 4) NULL,
    [rule_name]                                         VARCHAR (255)   NULL,
    [dummy_modified_date_time]                          DATETIME        NULL,
    [dv_load_date_time]                                 DATETIME        NOT NULL,
    [dv_r_load_source_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL,
    [dv_hash]                                           CHAR (32)       NOT NULL,
    [dv_deleted]                                        BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

