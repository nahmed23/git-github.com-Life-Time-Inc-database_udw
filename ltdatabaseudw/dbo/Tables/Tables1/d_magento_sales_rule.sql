﻿CREATE TABLE [dbo].[d_magento_sales_rule] (
    [d_magento_sales_rule_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [row_id]                  INT             NULL,
    [actions_serialized]      VARCHAR (8000)  NULL,
    [apply_to_shipping]       INT             NULL,
    [conditions_serialized]   VARCHAR (8000)  NULL,
    [coupon_type]             INT             NULL,
    [created_in]              BIGINT          NULL,
    [description]             VARCHAR (8000)  NULL,
    [discount_amount]         DECIMAL (12, 4) NULL,
    [discount_qty]            DECIMAL (12, 4) NULL,
    [discount_step]           INT             NULL,
    [from_date]               DATE            NULL,
    [from_dim_date_key]       CHAR (8)        NULL,
    [is_active_flag]          CHAR (1)        NULL,
    [is_advanced_flag]        CHAR (1)        NULL,
    [is_rss_flag]             CHAR (1)        NULL,
    [name]                    VARCHAR (255)   NULL,
    [product_ids]             VARCHAR (8000)  NULL,
    [rule_id]                 INT             NULL,
    [simple_action]           VARCHAR (32)    NULL,
    [simple_free_shipping]    INT             NULL,
    [sort_order]              INT             NULL,
    [stop_rules_processing]   INT             NULL,
    [times_used]              INT             NULL,
    [to_date]                 DATE            NULL,
    [to_dim_date_key]         CHAR (8)        NULL,
    [updated_in]              BIGINT          NULL,
    [use_auto_generation]     INT             NULL,
    [uses_per_coupon]         INT             NULL,
    [uses_per_customer]       INT             NULL,
    [p_magento_sales_rule_id] BIGINT          NOT NULL,
    [deleted_flag]            INT             NULL,
    [dv_load_date_time]       DATETIME        NULL,
    [dv_load_end_date_time]   DATETIME        NULL,
    [dv_batch_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

