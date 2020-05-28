CREATE TABLE [dbo].[s_spabiz_shift] (
    [s_spabiz_shift_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [shift_id]              DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [date]                  DATETIME        NULL,
    [day_id]                DECIMAL (26, 6) NULL,
    [time_open]             DATETIME        NULL,
    [time_close]            DATETIME        NULL,
    [time_rec]              DATETIME        NULL,
    [status]                DECIMAL (26, 6) NULL,
    [error_note]            VARCHAR (3000)  NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [amount_in_drawer]      DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_shift]
    ON [dbo].[s_spabiz_shift]([bk_hash] ASC, [s_spabiz_shift_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_shift]([dv_batch_id] ASC);

