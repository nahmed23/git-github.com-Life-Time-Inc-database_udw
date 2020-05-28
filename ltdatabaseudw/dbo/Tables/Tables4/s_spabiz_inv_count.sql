CREATE TABLE [dbo].[s_spabiz_inv_count] (
    [s_spabiz_inv_count_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [inv_count_id]          DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [num]                   VARCHAR (150)   NULL,
    [status]                DECIMAL (26, 6) NULL,
    [no_cycle]              DECIMAL (26, 6) NULL,
    [date_expected]         DATETIME        NULL,
    [date_started]          DATETIME        NULL,
    [date]                  DATETIME        NULL,
    [start_range]           VARCHAR (150)   NULL,
    [end_range]             VARCHAR (150)   NULL,
    [sort_count_by]         DECIMAL (26, 6) NULL,
    [name]                  VARCHAR (150)   NULL,
    [item_type]             DECIMAL (26, 6) NULL,
    [total_skus]            DECIMAL (26, 6) NULL,
    [num_adjusted]          DECIMAL (26, 6) NULL,
    [inv_effect]            DECIMAL (26, 6) NULL,
    [extra]                 VARCHAR (3000)  NULL,
    [edit_time]             DATETIME        NULL,
    [adj_num]               VARCHAR (150)   NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_inv_count]
    ON [dbo].[s_spabiz_inv_count]([bk_hash] ASC, [s_spabiz_inv_count_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_inv_count]([dv_batch_id] ASC);

