CREATE TABLE [dbo].[d_magento_customer_eav_attribute] (
    [d_magento_customer_eav_attribute_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [attribute_id]                        INT            NULL,
    [data_model]                          VARCHAR (255)  NULL,
    [input_filter]                        VARCHAR (255)  NULL,
    [is_filterable_in_grid]               CHAR (1)       NULL,
    [is_searchable_in_grid]               CHAR (1)       NULL,
    [is_system]                           CHAR (1)       NULL,
    [is_used_for_customer_segment]        CHAR (1)       NULL,
    [is_used_in_grid]                     CHAR (1)       NULL,
    [is_visible]                          CHAR (1)       NULL,
    [is_visible_in_grid]                  CHAR (1)       NULL,
    [multi_line_count]                    INT            NULL,
    [sort_order]                          INT            NULL,
    [validate_rules]                      VARCHAR (8000) NULL,
    [p_magento_customer_eav_attribute_id] BIGINT         NOT NULL,
    [deleted_flag]                        INT            NULL,
    [dv_load_date_time]                   DATETIME       NULL,
    [dv_load_end_date_time]               DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

