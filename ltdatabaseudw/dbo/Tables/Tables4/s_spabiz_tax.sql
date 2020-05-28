CREATE TABLE [dbo].[s_spabiz_tax] (
    [s_spabiz_tax_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [tax_id]                DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [tax_delete]            DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [quick_id]              VARCHAR (150)   NULL,
    [tax_auth_name]         VARCHAR (150)   NULL,
    [dept]                  VARCHAR (150)   NULL,
    [address_1]             VARCHAR (150)   NULL,
    [address_2]             VARCHAR (150)   NULL,
    [city]                  VARCHAR (150)   NULL,
    [state]                 VARCHAR (150)   NULL,
    [zip]                   VARCHAR (150)   NULL,
    [phone]                 VARCHAR (150)   NULL,
    [contact]               VARCHAR (150)   NULL,
    [contact_title]         VARCHAR (150)   NULL,
    [report_cycle]          DECIMAL (26, 6) NULL,
    [tax_type]              DECIMAL (26, 6) NULL,
    [amount]                DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_tax]
    ON [dbo].[s_spabiz_tax]([bk_hash] ASC, [s_spabiz_tax_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_tax]([dv_batch_id] ASC);

