CREATE TABLE [dbo].[s_hybris_promotion_action] (
    [s_hybris_promotion_action_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [hjmpts]                       BIGINT          NULL,
    [created_ts]                   DATETIME        NULL,
    [modified_ts]                  DATETIME        NULL,
    [promotion_action_pk]          BIGINT          NULL,
    [p_marked_applied]             TINYINT         NULL,
    [p_guid]                       NVARCHAR (255)  NULL,
    [acl_ts]                       BIGINT          NULL,
    [prop_ts]                      BIGINT          NULL,
    [p_amount]                     DECIMAL (26, 6) NULL,
    [p_order_entry_quantity]       BIGINT          NULL,
    [p_order_entry_number]         INT             NULL,
    [p_free_product]               BIGINT          NULL,
    [p_amoun0]                     DECIMAL (30, 8) NULL,
    [p_quantity]                   BIGINT          NULL,
    [p_delivery_cost]              DECIMAL (30, 8) NULL,
    [p_replaced_delivery_mode]     BIGINT          NULL,
    [p_replaced_delivery_cost]     DECIMAL (30, 8) NULL,
    [p_parameters]                 NVARCHAR (4000) NULL,
    [p_coupon_code]                NVARCHAR (255)  NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_r_load_source_id]          BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_hash]                      CHAR (32)       NOT NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_promotion_action]
    ON [dbo].[s_hybris_promotion_action]([bk_hash] ASC, [s_hybris_promotion_action_id] ASC);

