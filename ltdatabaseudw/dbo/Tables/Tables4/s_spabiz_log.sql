CREATE TABLE [dbo].[s_spabiz_log] (
    [s_spabiz_log_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ap_data_id]            DECIMAL (26, 6) NULL,
    [log_id]                DECIMAL (26, 6) NULL,
    [action]                DECIMAL (26, 6) NULL,
    [timestamp]             DATETIME        NULL,
    [start_time]            DATETIME        NULL,
    [end_time]              DATETIME        NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_log]
    ON [dbo].[s_spabiz_log]([bk_hash] ASC, [s_spabiz_log_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_log]([dv_batch_id] ASC);

