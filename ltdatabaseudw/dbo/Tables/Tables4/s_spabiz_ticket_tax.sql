CREATE TABLE [dbo].[s_spabiz_ticket_tax] (
    [s_spabiz_ticket_tax_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [ticket_tax_id]          DECIMAL (26, 6) NULL,
    [edit_time]              DATETIME        NULL,
    [amount]                 DECIMAL (26, 6) NULL,
    [date]                   DATETIME        NULL,
    [status]                 DECIMAL (26, 6) NULL,
    [cost]                   DECIMAL (26, 6) NULL,
    [store_number]           DECIMAL (26, 6) NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_ticket_tax]
    ON [dbo].[s_spabiz_ticket_tax]([bk_hash] ASC, [s_spabiz_ticket_tax_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ticket_tax]([dv_batch_id] ASC);

