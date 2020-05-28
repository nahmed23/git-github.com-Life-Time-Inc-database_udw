CREATE TABLE [dbo].[s_hybris_promotion] (
    [s_hybris_promotion_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [hjmpts]                             BIGINT          NULL,
    [created_ts]                         DATETIME        NULL,
    [modified_ts]                        DATETIME        NULL,
    [promotion_pk]                       BIGINT          NULL,
    [p_code]                             NVARCHAR (255)  NULL,
    [p_title]                            NVARCHAR (255)  NULL,
    [p_description]                      VARCHAR (8000)  NULL,
    [p_start_date]                       DATETIME        NULL,
    [p_end_date]                         DATETIME        NULL,
    [p_details_url]                      NVARCHAR (255)  NULL,
    [p_enabled]                          TINYINT         NULL,
    [p_priority]                         INT             NULL,
    [p_immutable_key_hash]               NVARCHAR (255)  NULL,
    [p_immutable_key]                    VARCHAR (8000)  NULL,
    [acl_ts]                             BIGINT          NULL,
    [prop_ts]                            BIGINT          NULL,
    [p_product_fixed_unit_price]         VARCHAR (8000)  NULL,
    [p_percentage_discount]              DECIMAL (26, 6) NULL,
    [p_qualifying_count]                 INT             NULL,
    [p_free_count]                       INT             NULL,
    [p_bundle_prices]                    VARCHAR (8000)  NULL,
    [p_qualifying_counts_and_bundle_pri] VARCHAR (8000)  NULL,
    [p_partner_products]                 VARCHAR (8000)  NULL,
    [p_partner_prices]                   VARCHAR (8000)  NULL,
    [p_threshold_totals]                 VARCHAR (8000)  NULL,
    [p_product_prices]                   VARCHAR (8000)  NULL,
    [p_include_discounted_price_in_thre] TINYINT         NULL,
    [p_discount_prices]                  VARCHAR (8000)  NULL,
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
CREATE CLUSTERED INDEX [ci_s_hybris_promotion]
    ON [dbo].[s_hybris_promotion]([bk_hash] ASC, [s_hybris_promotion_id] ASC);

