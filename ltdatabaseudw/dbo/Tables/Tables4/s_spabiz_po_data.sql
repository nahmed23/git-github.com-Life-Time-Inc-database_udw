CREATE TABLE [dbo].[s_spabiz_po_data] (
    [s_spabiz_po_data_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [po_data_id]            DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [date]                  DATETIME        NULL,
    [type]                  DECIMAL (26, 6) NULL,
    [line_num]              DECIMAL (26, 6) NULL,
    [normal_cost]           DECIMAL (26, 6) NULL,
    [cost]                  DECIMAL (26, 6) NULL,
    [ext_cost]              DECIMAL (26, 6) NULL,
    [qty_ord]               DECIMAL (26, 6) NULL,
    [qty_rec]               DECIMAL (26, 6) NULL,
    [status]                DECIMAL (26, 6) NULL,
    [margin]                DECIMAL (26, 6) NULL,
    [retail_price]          DECIMAL (26, 6) NULL,
    [name]                  VARCHAR (150)   NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_po_data]
    ON [dbo].[s_spabiz_po_data]([bk_hash] ASC, [s_spabiz_po_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_po_data]([dv_batch_id] ASC);

