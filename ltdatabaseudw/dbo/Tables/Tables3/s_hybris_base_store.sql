CREATE TABLE [dbo].[s_hybris_base_store] (
    [s_hybris_base_store_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [hjmpts]                             BIGINT          NULL,
    [created_ts]                         DATETIME        NULL,
    [modified_ts]                        DATETIME        NULL,
    [base_store_pk]                      BIGINT          NULL,
    [p_uid]                              NVARCHAR (255)  NULL,
    [p_net]                              TINYINT         NULL,
    [p_submit_order_process_code]        NVARCHAR (255)  NULL,
    [p_create_return_process_code]       NVARCHAR (255)  NULL,
    [p_external_tax_enabled]             TINYINT         NULL,
    [p_max_radius_for_pos_search]        DECIMAL (26, 6) NULL,
    [p_customer_allowed_to_ignore_sugge] TINYINT         NULL,
    [p_payment_provider]                 NVARCHAR (255)  NULL,
    [p_express_checkout_enabled]         TINYINT         NULL,
    [p_tax_estimation_enabled]           TINYINT         NULL,
    [p_checkout_flow_group]              NVARCHAR (255)  NULL,
    [acl_ts]                             BIGINT          NULL,
    [prop_ts]                            BIGINT          NULL,
    [dv_load_date_time]                  DATETIME        NOT NULL,
    [dv_r_load_source_id]                BIGINT          NOT NULL,
    [dv_inserted_date_time]              DATETIME        NOT NULL,
    [dv_insert_user]                     VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]               DATETIME        NULL,
    [dv_update_user]                     VARCHAR (50)    NULL,
    [dv_hash]                            CHAR (32)       NOT NULL,
    [dv_batch_id]                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_base_store]
    ON [dbo].[s_hybris_base_store]([bk_hash] ASC, [s_hybris_base_store_id] ASC);

