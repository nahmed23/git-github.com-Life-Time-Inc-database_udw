﻿CREATE TABLE [dbo].[l_spabiz_data_type] (
    [l_spabiz_data_type_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [data_type_id]          DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_spabiz_data_type]
    ON [dbo].[l_spabiz_data_type]([bk_hash] ASC, [l_spabiz_data_type_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_data_type]([dv_batch_id] ASC);

