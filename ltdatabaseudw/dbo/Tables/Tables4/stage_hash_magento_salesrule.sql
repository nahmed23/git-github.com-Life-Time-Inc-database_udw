﻿CREATE TABLE [dbo].[stage_hash_magento_salesrule] (
    [stage_hash_magento_salesrule_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [row_id]                          INT             NULL,
    [rule_id]                         INT             NULL,
    [created_in]                      BIGINT          NULL,
    [updated_in]                      BIGINT          NULL,
    [name]                            VARCHAR (255)   NULL,
    [description]                     VARCHAR (8000)  NULL,
    [from_date]                       DATE            NULL,
    [to_date]                         DATE            NULL,
    [uses_per_customer]               INT             NULL,
    [is_active]                       INT             NULL,
    [conditions_serialized]           VARCHAR (8000)  NULL,
    [actions_serialized]              VARCHAR (8000)  NULL,
    [stop_rules_processing]           INT             NULL,
    [is_advanced]                     INT             NULL,
    [product_ids]                     VARCHAR (8000)  NULL,
    [sort_order]                      INT             NULL,
    [simple_action]                   VARCHAR (32)    NULL,
    [discount_amount]                 DECIMAL (12, 4) NULL,
    [discount_qty]                    DECIMAL (12, 4) NULL,
    [discount_step]                   INT             NULL,
    [apply_to_shipping]               INT             NULL,
    [times_used]                      INT             NULL,
    [is_rss]                          INT             NULL,
    [coupon_type]                     INT             NULL,
    [use_auto_generation]             INT             NULL,
    [uses_per_coupon]                 INT             NULL,
    [simple_free_shipping]            INT             NULL,
    [dummy_modified_date_time]        DATETIME        NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

