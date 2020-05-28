CREATE TABLE [dbo].[d_spabiz_category] (
    [d_spabiz_category_id]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [dim_spabiz_category_key]      CHAR (32)       NULL,
    [category_id]                  DECIMAL (26, 6) NULL,
    [store_number]                 DECIMAL (26, 6) NULL,
    [category_name]                VARCHAR (150)   NULL,
    [deleted_date_time]            DATETIME        NULL,
    [deleted_flag]                 CHAR (1)        NULL,
    [dim_spabiz_data_type_key]     CHAR (32)       NULL,
    [dim_spabiz_store_key]         CHAR (32)       NULL,
    [edit_date_time]               DATETIME        NULL,
    [parent_category_bk_hash]      CHAR (32)       NULL,
    [sub_category_flag]            CHAR (1)        NULL,
    [l_spabiz_category_gl_account] VARCHAR (90)    NULL,
    [l_spabiz_category_parent_id]  DECIMAL (26, 6) NULL,
    [p_spabiz_category_id]         BIGINT          NOT NULL,
    [dv_load_date_time]            DATETIME        NULL,
    [dv_load_end_date_time]        DATETIME        NULL,
    [dv_batch_id]                  BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_category]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_category]([dv_batch_id]);

