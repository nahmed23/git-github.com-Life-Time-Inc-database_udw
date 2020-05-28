CREATE TABLE [dbo].[s_spabiz_category] (
    [s_spabiz_category_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [category_id]           DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [deleted]               DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [quick_id]              VARCHAR (150)   NULL,
    [fast_index]            VARCHAR (30)    NULL,
    [cosmetic]              DECIMAL (26, 6) NULL,
    [display_color]         VARCHAR (150)   NULL,
    [lvl]                   DECIMAL (26, 6) NULL,
    [level]                 DECIMAL (26, 6) NULL,
    [web_book]              DECIMAL (26, 6) NULL,
    [web_view]              DECIMAL (26, 6) NULL,
    [class]                 VARCHAR (3)     NULL,
    [department]            DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_category]
    ON [dbo].[s_spabiz_category]([bk_hash] ASC, [s_spabiz_category_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_category]([dv_batch_id] ASC);

