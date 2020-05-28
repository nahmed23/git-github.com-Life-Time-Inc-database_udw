CREATE TABLE [dbo].[l_spabiz_ro] (
    [l_spabiz_ro_id]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ro_id]                 DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [vendor_id]             DECIMAL (26, 6) NULL,
    [staff_id]              DECIMAL (26, 6) NULL,
    [check_staff_id]        DECIMAL (26, 6) NULL,
    [stock_staff_id]        DECIMAL (26, 6) NULL,
    [po_id]                 DECIMAL (26, 6) NULL,
    [po_num]                VARCHAR (150)   NULL,
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
CREATE CLUSTERED INDEX [ci_l_spabiz_ro]
    ON [dbo].[l_spabiz_ro]([bk_hash] ASC, [l_spabiz_ro_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_ro]([dv_batch_id] ASC);

