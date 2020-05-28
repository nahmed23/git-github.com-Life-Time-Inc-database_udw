CREATE TABLE [dbo].[s_hybris_orders] (
    [s_hybris_orders_id]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [hjmpts]                            BIGINT          NULL,
    [created_ts]                        DATETIME        NULL,
    [modified_ts]                       DATETIME        NULL,
    [orders_pk]                         BIGINT          NULL,
    [p_calculated]                      TINYINT         NULL,
    [p_code]                            NVARCHAR (255)  NULL,
    [p_delivery_cost]                   DECIMAL (30, 8) NULL,
    [p_global_discount_values_internal] VARCHAR (8000)  NULL,
    [p_net]                             TINYINT         NULL,
    [p_payment_cost]                    DECIMAL (30, 8) NULL,
    [p_status_info]                     NVARCHAR (255)  NULL,
    [p_total_price]                     DECIMAL (30, 8) NULL,
    [p_total_discounts]                 DECIMAL (30, 8) NULL,
    [p_total_tax]                       DECIMAL (30, 8) NULL,
    [p_total_tax_values_internal]       VARCHAR (8000)  NULL,
    [p_subtotal]                        DECIMAL (30, 8) NULL,
    [p_discounts_include_delivery_cost] TINYINT         NULL,
    [p_discounts_include_payment_cost]  TINYINT         NULL,
    [p_guid]                            NVARCHAR (255)  NULL,
    [p_version_id]                      NVARCHAR (255)  NULL,
    [p_fraudulent]                      TINYINT         NULL,
    [p_potentially_fraudulent]          TINYINT         NULL,
    [acl_ts]                            BIGINT          NULL,
    [prop_ts]                           BIGINT          NULL,
    [p_notes]                           NVARCHAR (255)  NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_r_load_source_id]               BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_hash]                           CHAR (32)       NOT NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_orders]
    ON [dbo].[s_hybris_orders]([bk_hash] ASC, [s_hybris_orders_id] ASC);

