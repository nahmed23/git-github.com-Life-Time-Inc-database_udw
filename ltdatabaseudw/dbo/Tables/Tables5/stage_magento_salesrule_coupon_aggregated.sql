CREATE TABLE [dbo].[stage_magento_salesrule_coupon_aggregated] (
    [stage_magento_salesrule_coupon_aggregated_id] BIGINT          NOT NULL,
    [id]                                           INT             NULL,
    [period]                                       DATE            NULL,
    [store_id]                                     INT             NULL,
    [order_status]                                 VARCHAR (50)    NULL,
    [coupon_code]                                  VARCHAR (50)    NULL,
    [coupon_uses]                                  INT             NULL,
    [subtotal_amount]                              DECIMAL (12, 4) NULL,
    [discount_amount]                              DECIMAL (12, 4) NULL,
    [total_amount]                                 DECIMAL (12, 4) NULL,
    [subtotal_amount_actual]                       DECIMAL (12, 4) NULL,
    [discount_amount_actual]                       DECIMAL (12, 4) NULL,
    [total_amount_actual]                          DECIMAL (12, 4) NULL,
    [rule_name]                                    VARCHAR (255)   NULL,
    [dummy_modified_date_time]                     DATETIME        NULL,
    [dv_batch_id]                                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

