CREATE TABLE [dbo].[s_spabiz_gl_setup] (
    [s_spabiz_gl_setup_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [description]           VARCHAR (150)   NULL,
    [gl_account]            VARCHAR (60)    NULL,
    [edit_time]             DATETIME        NULL,
    [status]                DECIMAL (26, 6) NULL,
    [deleted]               DECIMAL (26, 6) NULL,
    [expense]               DECIMAL (26, 6) NULL,
    [optional]              DECIMAL (26, 6) NULL,
    [rank]                  DECIMAL (26, 6) NULL,
    [gl_setup_id]           DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [store_id]              DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_gl_setup]
    ON [dbo].[s_spabiz_gl_setup]([bk_hash] ASC, [s_spabiz_gl_setup_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_gl_setup]([dv_batch_id] ASC);

