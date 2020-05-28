CREATE TABLE [dbo].[d_spabiz_series_data] (
    [d_spabiz_series_data_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [fact_spabiz_series_sold_instance_key] CHAR (32)       NULL,
    [series_data_id]                       BIGINT          NULL,
    [store_number]                         BIGINT          NULL,
    [dim_spabiz_series_key]                CHAR (32)       NULL,
    [dim_spabiz_service_key]               CHAR (32)       NULL,
    [dim_spabiz_store_key]                 CHAR (32)       NULL,
    [edit_date_time]                       DATETIME        NULL,
    [price_type_dim_description_key]       VARCHAR (50)    NULL,
    [price_type_id]                        VARCHAR (50)    NULL,
    [service_price]                        DECIMAL (26, 6) NULL,
    [l_spabiz_series_data_series_id]       BIGINT          NULL,
    [l_spabiz_series_data_service_id]      BIGINT          NULL,
    [s_spabiz_series_data_price_type]      DECIMAL (26, 6) NULL,
    [p_spabiz_series_data_id]              BIGINT          NOT NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_series_data]([dv_batch_id] ASC);

