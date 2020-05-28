﻿CREATE TABLE [dbo].[d_spabiz_po] (
    [d_spabiz_po_id]                 BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [fact_spabiz_purchase_order_key] CHAR (32)       NULL,
    [po_id]                          BIGINT          NULL,
    [store_number]                   BIGINT          NULL,
    [created_date_time]              DATETIME        NULL,
    [dim_spabiz_staff_key]           CHAR (32)       NULL,
    [dim_spabiz_store_key]           CHAR (32)       NULL,
    [dim_spabiz_vendor_key]          CHAR (32)       NULL,
    [discount]                       DECIMAL (26, 6) NULL,
    [edit_date_time]                 DATETIME        NULL,
    [payment]                        VARCHAR (150)   NULL,
    [retail_total]                   DECIMAL (26, 6) NULL,
    [status_dim_description_key]     VARCHAR (50)    NULL,
    [status_id]                      VARCHAR (50)    NULL,
    [sub_total]                      DECIMAL (26, 6) NULL,
    [tax]                            DECIMAL (26, 6) NULL,
    [total]                          DECIMAL (26, 6) NULL,
    [l_spabiz_po_staff_id]           BIGINT          NULL,
    [l_spabiz_po_vendor_id]          BIGINT          NULL,
    [s_spabiz_po_status]             DECIMAL (26, 6) NULL,
    [p_spabiz_po_id]                 BIGINT          NOT NULL,
    [dv_load_date_time]              DATETIME        NULL,
    [dv_load_end_date_time]          DATETIME        NULL,
    [dv_batch_id]                    BIGINT          NOT NULL,
    [dv_inserted_date_time]          DATETIME        NOT NULL,
    [dv_insert_user]                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_po]([dv_batch_id] ASC);

