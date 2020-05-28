CREATE TABLE [dbo].[d_spabiz_inv_adj_data] (
    [d_spabiz_inv_adj_data_id]                   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)       NOT NULL,
    [fact_spabiz_inventory_adjustment_item_key]  CHAR (32)       NULL,
    [inv_adj_data_id]                            BIGINT          NULL,
    [store_number]                               BIGINT          NULL,
    [cost]                                       DECIMAL (26, 6) NULL,
    [created_date_time]                          DATETIME        NULL,
    [dim_spabiz_category_key]                    CHAR (32)       NULL,
    [dim_spabiz_inventory_adjustment_reason_key] CHAR (32)       NULL,
    [dim_spabiz_product_key]                     CHAR (32)       NULL,
    [dim_spabiz_staff_key]                       CHAR (32)       NULL,
    [dim_spabiz_store_key]                       CHAR (32)       NULL,
    [edit_date_time]                             DATETIME        NULL,
    [quantity]                                   BIGINT          NULL,
    [source_fact_spabiz_inventory_count_key]     CHAR (32)       NULL,
    [source_fact_spabiz_receiving_order_key]     CHAR (32)       NULL,
    [source_type_dim_description_key]            VARCHAR (50)    NULL,
    [source_type_id]                             VARCHAR (50)    NULL,
    [status_dim_description_key]                 VARCHAR (50)    NULL,
    [status_id]                                  VARCHAR (50)    NULL,
    [l_spabiz_inv_adj_data_adj_id]               BIGINT          NULL,
    [l_spabiz_inv_adj_data_cat_id]               BIGINT          NULL,
    [l_spabiz_inv_adj_data_product_id]           BIGINT          NULL,
    [l_spabiz_inv_adj_data_reason_id]            BIGINT          NULL,
    [l_spabiz_inv_adj_data_source_id]            INT             NULL,
    [l_spabiz_inv_adj_data_staff_id]             BIGINT          NULL,
    [s_spabiz_inv_adj_data_source_type]          INT             NULL,
    [s_spabiz_inv_adj_data_status]               INT             NULL,
    [p_spabiz_inv_adj_data_id]                   BIGINT          NOT NULL,
    [dv_load_date_time]                          DATETIME        NULL,
    [dv_load_end_date_time]                      DATETIME        NULL,
    [dv_batch_id]                                BIGINT          NOT NULL,
    [dv_inserted_date_time]                      DATETIME        NOT NULL,
    [dv_insert_user]                             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                       DATETIME        NULL,
    [dv_update_user]                             VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_inv_adj_data]([dv_batch_id] ASC);

