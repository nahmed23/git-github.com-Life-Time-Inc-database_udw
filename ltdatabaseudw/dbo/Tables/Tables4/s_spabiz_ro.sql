CREATE TABLE [dbo].[s_spabiz_ro] (
    [s_spabiz_ro_id]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ro_id]                 DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [type]                  DECIMAL (26, 6) NULL,
    [num]                   VARCHAR (150)   NULL,
    [date]                  DATETIME        NULL,
    [pack_num]              VARCHAR (150)   NULL,
    [status]                DECIMAL (26, 6) NULL,
    [payment]               VARCHAR (150)   NULL,
    [discount]              DECIMAL (26, 6) NULL,
    [tax]                   DECIMAL (26, 6) NULL,
    [total]                 DECIMAL (26, 6) NULL,
    [sub_total]             DECIMAL (26, 6) NULL,
    [freight]               DECIMAL (26, 6) NULL,
    [retail_total]          DECIMAL (26, 6) NULL,
    [tax_type]              DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [inv_date]              DATETIME        NULL,
    [inv_number]            VARCHAR (150)   NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_ro]
    ON [dbo].[s_spabiz_ro]([bk_hash] ASC, [s_spabiz_ro_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ro]([dv_batch_id] ASC);

