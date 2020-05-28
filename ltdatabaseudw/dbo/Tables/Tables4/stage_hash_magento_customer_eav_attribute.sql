CREATE TABLE [dbo].[stage_hash_magento_customer_eav_attribute] (
    [stage_hash_magento_customer_eav_attribute_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)      NOT NULL,
    [attribute_id]                                 INT            NULL,
    [is_visible]                                   INT            NULL,
    [input_filter]                                 VARCHAR (255)  NULL,
    [multiline_count]                              INT            NULL,
    [validate_rules]                               VARCHAR (8000) NULL,
    [is_system]                                    INT            NULL,
    [sort_order]                                   INT            NULL,
    [data_model]                                   VARCHAR (255)  NULL,
    [is_used_in_grid]                              INT            NULL,
    [is_visible_in_grid]                           INT            NULL,
    [is_filterable_in_grid]                        INT            NULL,
    [is_searchable_in_grid]                        INT            NULL,
    [is_used_for_customer_segment]                 INT            NULL,
    [dummy_modified_date_time]                     DATETIME       NULL,
    [dv_load_date_time]                            DATETIME       NOT NULL,
    [dv_batch_id]                                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

