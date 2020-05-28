CREATE TABLE [dbo].[s_spabiz_drawer_entry] (
    [s_spabiz_drawer_entry_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [drawer_entry_id]          DECIMAL (26, 6) NULL,
    [counter_id]               DECIMAL (26, 6) NULL,
    [edit_time]                DATETIME        NULL,
    [status]                   DECIMAL (26, 6) NULL,
    [num]                      VARCHAR (60)    NULL,
    [in_amount]                DECIMAL (26, 6) NULL,
    [in_ok]                    DECIMAL (26, 6) NULL,
    [out_amount]               DECIMAL (26, 6) NULL,
    [out_ok]                   DECIMAL (26, 6) NULL,
    [date]                     DATETIME        NULL,
    [time]                     DATETIME        NULL,
    [payee_type]               DECIMAL (26, 6) NULL,
    [payee_index]              VARCHAR (150)   NULL,
    [note]                     VARCHAR (150)   NULL,
    [ok]                       DECIMAL (26, 6) NULL,
    [check_num]                VARCHAR (60)    NULL,
    [drawer_num]               VARCHAR (150)   NULL,
    [store_number]             DECIMAL (26, 6) NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_batch_id]              BIGINT          NOT NULL,
    [dv_r_load_source_id]      BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_hash]                  CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_drawer_entry]
    ON [dbo].[s_spabiz_drawer_entry]([bk_hash] ASC, [s_spabiz_drawer_entry_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_drawer_entry]([dv_batch_id] ASC);

