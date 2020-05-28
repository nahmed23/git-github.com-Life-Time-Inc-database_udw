﻿CREATE TABLE [dbo].[d_spabiz_vendor_mapping] (
    [d_spabiz_vendor_mapping_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [dim_spabiz_vendor_mapping_key] CHAR (32)    NULL,
    [spabiz_vendor_database_id]     BIGINT       NULL,
    [vendor_mapping_id]             BIGINT       NULL,
    [workday_supplier_id]           BIGINT       NULL,
    [p_spabiz_vendor_mapping_id]    BIGINT       NOT NULL,
    [dv_load_date_time]             DATETIME     NULL,
    [dv_load_end_date_time]         DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_vendor_mapping]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_vendor_mapping]([dv_batch_id]);

