CREATE TABLE [dbo].[stage_hash_magento_customer_eav_attribute_website] (
    [stage_hash_magento_customer_eav_attribute_website_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                              CHAR (32)      NOT NULL,
    [attribute_id]                                         INT            NULL,
    [website_id]                                           INT            NULL,
    [is_visible]                                           INT            NULL,
    [is_required]                                          INT            NULL,
    [default_value]                                        VARCHAR (8000) NULL,
    [multiline_count]                                      INT            NULL,
    [dummy_modified_date_time]                             DATETIME       NULL,
    [dv_load_date_time]                                    DATETIME       NOT NULL,
    [dv_batch_id]                                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

