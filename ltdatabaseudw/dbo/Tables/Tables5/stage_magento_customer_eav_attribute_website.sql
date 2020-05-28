CREATE TABLE [dbo].[stage_magento_customer_eav_attribute_website] (
    [stage_magento_customer_eav_attribute_website_id] BIGINT         NOT NULL,
    [attribute_id]                                    INT            NULL,
    [website_id]                                      INT            NULL,
    [is_visible]                                      INT            NULL,
    [is_required]                                     INT            NULL,
    [default_value]                                   VARCHAR (8000) NULL,
    [multiline_count]                                 INT            NULL,
    [dummy_modified_date_time]                        DATETIME       NULL,
    [dv_batch_id]                                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

