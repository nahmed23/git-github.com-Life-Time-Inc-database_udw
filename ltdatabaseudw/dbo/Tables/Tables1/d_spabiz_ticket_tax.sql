﻿CREATE TABLE [dbo].[d_spabiz_ticket_tax] (
    [d_spabiz_ticket_tax_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [fact_spabiz_ticket_tax_key] CHAR (32)       NULL,
    [ticket_tax_id]              BIGINT          NULL,
    [store_number]               BIGINT          NULL,
    [amount]                     DECIMAL (26, 6) NULL,
    [created_date_time]          DATETIME        NULL,
    [dim_spabiz_shift_key]       CHAR (32)       NULL,
    [dim_spabiz_store_key]       CHAR (32)       NULL,
    [dim_spabiz_tax_rate_key]    CHAR (32)       NULL,
    [edit_date_time]             DATETIME        NULL,
    [fact_spabiz_ticket_key]     CHAR (32)       NULL,
    [status_dim_description_key] VARCHAR (50)    NULL,
    [status_id]                  VARCHAR (50)    NULL,
    [p_spabiz_ticket_tax_id]     BIGINT          NOT NULL,
    [dv_load_date_time]          DATETIME        NULL,
    [dv_load_end_date_time]      DATETIME        NULL,
    [dv_batch_id]                BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ticket_tax]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_ticket_tax]([dv_batch_id]);

