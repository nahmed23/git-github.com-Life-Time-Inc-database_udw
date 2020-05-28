﻿CREATE TABLE [dbo].[d_spabiz_data_type] (
    [d_spabiz_data_type_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)     NOT NULL,
    [dim_spabiz_data_type_key] CHAR (32)     NULL,
    [data_type_id]             BIGINT        NULL,
    [store_number]             BIGINT        NULL,
    [data_type_name]           VARCHAR (150) NULL,
    [edit_date_time]           DATETIME      NULL,
    [p_spabiz_data_type_id]    BIGINT        NOT NULL,
    [dv_load_date_time]        DATETIME      NULL,
    [dv_load_end_date_time]    DATETIME      NULL,
    [dv_batch_id]              BIGINT        NOT NULL,
    [dv_inserted_date_time]    DATETIME      NOT NULL,
    [dv_insert_user]           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]     DATETIME      NULL,
    [dv_update_user]           VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_data_type]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_data_type]([dv_batch_id]);

