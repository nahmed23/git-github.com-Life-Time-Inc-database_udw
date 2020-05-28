CREATE TABLE [dbo].[d_spabiz_po_data] (
    [d_spabiz_po_data_id]                 BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_spabiz_purchase_order_item_key] CHAR (32)       NULL,
    [po_data_id]                          BIGINT          NULL,
    [store_number]                        BIGINT          NULL,
    [cost]                                DECIMAL (26, 6) NULL,
    [created_date_time]                   DATETIME        NULL,
    [dim_spabiz_category_key]             CHAR (32)       NULL,
    [dim_spabiz_product_key]              CHAR (32)       NULL,
    [dim_spabiz_store_key]                CHAR (32)       NULL,
    [dim_spabiz_vendor_key]               CHAR (32)       NULL,
    [edit_date_time]                      DATETIME        NULL,
    [external_cost]                       DECIMAL (26, 6) NULL,
    [fact_spabiz_purchase_order_key]      CHAR (32)       NULL,
    [items_ordered]                       INT             NULL,
    [items_received]                      INT             NULL,
    [line_number]                         INT             NULL,
    [margin]                              DECIMAL (26, 6) NULL,
    [retail_price]                        DECIMAL (26, 6) NULL,
    [status_dim_description_key]          VARCHAR (50)    NULL,
    [status_id]                           VARCHAR (50)    NULL,
    [type_dim_description_key]            VARCHAR (50)    NULL,
    [type_id]                             VARCHAR (50)    NULL,
    [l_spabiz_po_data_cat_id]             BIGINT          NULL,
    [l_spabiz_po_data_po_id]              BIGINT          NULL,
    [l_spabiz_po_data_product_id]         BIGINT          NULL,
    [l_spabiz_po_data_vendor_id]          BIGINT          NULL,
    [s_spabiz_po_data_status]             DECIMAL (26, 6) NULL,
    [s_spabiz_po_data_type]               DECIMAL (26, 6) NULL,
    [p_spabiz_po_data_id]                 BIGINT          NOT NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_po_data]([dv_batch_id] ASC);

