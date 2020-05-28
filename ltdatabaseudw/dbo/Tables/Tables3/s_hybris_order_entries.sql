CREATE TABLE [dbo].[s_hybris_order_entries] (
    [s_hybris_order_entries_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [hjmpts]                     BIGINT          NULL,
    [created_ts]                 DATETIME        NULL,
    [modified_ts]                DATETIME        NULL,
    [order_entries_pk]           BIGINT          NULL,
    [p_base_price]               DECIMAL (30, 8) NULL,
    [p_calculated]               TINYINT         NULL,
    [p_discount_values_internal] VARCHAR (8000)  NULL,
    [p_entry_number]             INT             NULL,
    [p_info]                     VARCHAR (8000)  NULL,
    [p_quantity]                 DECIMAL (30, 8) NULL,
    [p_tax_values_internal]      NVARCHAR (255)  NULL,
    [p_total_price]              DECIMAL (30, 8) NULL,
    [p_give_away]                TINYINT         NULL,
    [p_rejected]                 TINYINT         NULL,
    [p_named_delivery_date]      DATETIME        NULL,
    [p_xml_product]              VARCHAR (8000)  NULL,
    [p_original_subscription_id] NVARCHAR (255)  NULL,
    [p_bundle_no]                INT             NULL,
    [acl_ts]                     BIGINT          NULL,
    [prop_ts]                    BIGINT          NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_r_load_source_id]        BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_hash]                    CHAR (32)       NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_order_entries]
    ON [dbo].[s_hybris_order_entries]([bk_hash] ASC, [s_hybris_order_entries_id] ASC);

