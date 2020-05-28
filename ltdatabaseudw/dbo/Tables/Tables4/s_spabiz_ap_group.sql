CREATE TABLE [dbo].[s_spabiz_ap_group] (
    [s_spabiz_ap_group_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ap_group_id]           DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [deleted]               DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [cols]                  DECIMAL (26, 6) NULL,
    [quick_key]             VARCHAR (150)   NULL,
    [tab]                   DECIMAL (26, 6) NULL,
    [tab_order]             DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_ap_group]
    ON [dbo].[s_spabiz_ap_group]([bk_hash] ASC, [s_spabiz_ap_group_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ap_group]([dv_batch_id] ASC);

