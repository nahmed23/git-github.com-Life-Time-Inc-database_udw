CREATE TABLE [dbo].[dim_spabiz_series] (
    [dim_spabiz_series_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_spabiz_series_key] CHAR (32)     NULL,
    [series_id]             BIGINT        NULL,
    [store_number]          BIGINT        NULL,
    [category]              VARCHAR (25)  NULL,
    [deleted_date_time]     DATETIME      NULL,
    [deleted_flag]          CHAR (1)      NULL,
    [dim_spabiz_store_key]  CHAR (32)     NULL,
    [edit_date_time]        DATETIME      NULL,
    [quick_id]              VARCHAR (150) NULL,
    [segment]               VARCHAR (25)  NULL,
    [series_name]           VARCHAR (150) NULL,
    [taxable_flag]          CHAR (1)      NULL,
    [p_spabiz_series_id]    BIGINT        NULL,
    [dv_load_date_time]     DATETIME      NULL,
    [dv_load_end_date_time] DATETIME      NULL,
    [dv_batch_id]           BIGINT        NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_spabiz_series_key]));

