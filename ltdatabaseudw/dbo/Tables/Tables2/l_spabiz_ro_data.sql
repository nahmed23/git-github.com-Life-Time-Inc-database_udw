﻿CREATE TABLE [dbo].[l_spabiz_ro_data] (
    [l_spabiz_ro_data_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ro_data_id]            DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [ro_id]                 DECIMAL (26, 6) NULL,
    [po_id]                 DECIMAL (26, 6) NULL,
    [po_line_id]            DECIMAL (26, 6) NULL,
    [vendor_id]             DECIMAL (26, 6) NULL,
    [product_id]            DECIMAL (26, 6) NULL,
    [cat_id]                DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_l_spabiz_ro_data]
    ON [dbo].[l_spabiz_ro_data]([bk_hash] ASC, [l_spabiz_ro_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_ro_data]([dv_batch_id] ASC);

