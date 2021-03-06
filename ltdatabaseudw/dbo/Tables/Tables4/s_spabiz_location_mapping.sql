﻿CREATE TABLE [dbo].[s_spabiz_location_mapping] (
    [s_spabiz_location_mapping_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [name]                         VARCHAR (4000) NULL,
    [spabiz_store_number]          BIGINT         NULL,
    [jan_one]                      DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_r_load_source_id]          BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_hash]                      CHAR (32)      NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_location_mapping]
    ON [dbo].[s_spabiz_location_mapping]([bk_hash] ASC, [s_spabiz_location_mapping_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_location_mapping]([dv_batch_id] ASC);

