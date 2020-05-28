﻿CREATE TABLE [dbo].[s_magento_customer_eav_attribute] (
    [s_magento_customer_eav_attribute_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [attribute_id]                        INT            NULL,
    [is_visible]                          INT            NULL,
    [input_filter]                        VARCHAR (255)  NULL,
    [multi_line_count]                    INT            NULL,
    [validate_rules]                      VARCHAR (8000) NULL,
    [is_system]                           INT            NULL,
    [sort_order]                          INT            NULL,
    [data_model]                          VARCHAR (255)  NULL,
    [is_used_in_grid]                     INT            NULL,
    [is_visible_in_grid]                  INT            NULL,
    [is_filterable_in_grid]               INT            NULL,
    [is_searchable_in_grid]               INT            NULL,
    [is_used_for_customer_segment]        INT            NULL,
    [dummy_modified_date_time]            DATETIME       NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_r_load_source_id]                 BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL,
    [dv_hash]                             CHAR (32)      NOT NULL,
    [dv_deleted]                          BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

