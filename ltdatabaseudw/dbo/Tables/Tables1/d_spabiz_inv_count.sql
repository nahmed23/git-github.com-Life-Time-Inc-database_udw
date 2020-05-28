CREATE TABLE [dbo].[d_spabiz_inv_count] (
    [d_spabiz_inv_count_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [fact_spabiz_inventory_count_key]   CHAR (32)       NULL,
    [inv_count_id]                      BIGINT          NULL,
    [store_number]                      BIGINT          NULL,
    [created_date_time]                 DATETIME        NULL,
    [date_expected_date_time]           DATETIME        NULL,
    [date_started_date_time]            DATETIME        NULL,
    [dim_spabiz_staff_key]              CHAR (32)       NULL,
    [dim_spabiz_store_key]              CHAR (32)       NULL,
    [edit_date_time]                    DATETIME        NULL,
    [end_range]                         VARCHAR (150)   NULL,
    [extra_query_text_filter]           VARCHAR (3000)  NULL,
    [inventory_count]                   INT             NULL,
    [inventory_count_adjustment]        INT             NULL,
    [inventory_effect]                  DECIMAL (26, 6) NULL,
    [item_type_dim_description_key]     VARCHAR (50)    NULL,
    [item_type_id]                      VARCHAR (50)    NULL,
    [name]                              VARCHAR (3000)  NULL,
    [sort_count_by_dim_description_key] VARCHAR (50)    NULL,
    [sort_count_by_id]                  VARCHAR (50)    NULL,
    [start_range]                       VARCHAR (150)   NULL,
    [status_dim_description_key]        VARCHAR (50)    NULL,
    [status_id]                         VARCHAR (50)    NULL,
    [total_skus]                        INT             NULL,
    [l_spabiz_inv_count_staff_id]       BIGINT          NULL,
    [p_spabiz_inv_count_id]             BIGINT          NOT NULL,
    [dv_load_date_time]                 DATETIME        NULL,
    [dv_load_end_date_time]             DATETIME        NULL,
    [dv_batch_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_inv_count]([dv_batch_id] ASC);

