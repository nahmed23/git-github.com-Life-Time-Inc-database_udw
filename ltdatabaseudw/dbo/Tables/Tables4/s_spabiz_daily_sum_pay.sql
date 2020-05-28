CREATE TABLE [dbo].[s_spabiz_daily_sum_pay] (
    [s_spabiz_daily_sum_pay_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [daily_sum_pay_id]          DECIMAL (26, 6) NULL,
    [counter_id]                DECIMAL (26, 6) NULL,
    [edit_time]                 DATETIME        NULL,
    [date]                      DATETIME        NULL,
    [start_amount]              DECIMAL (26, 6) NULL,
    [ticket_amt]                DECIMAL (26, 6) NULL,
    [change_out]                DECIMAL (26, 6) NULL,
    [drawer_entries]            DECIMAL (26, 6) NULL,
    [you_have]                  DECIMAL (26, 6) NULL,
    [error]                     DECIMAL (26, 6) NULL,
    [deposit]                   DECIMAL (26, 6) NULL,
    [total]                     DECIMAL (26, 6) NULL,
    [store_number]              DECIMAL (26, 6) NULL,
    [dv_load_date_time]         DATETIME        NOT NULL,
    [dv_batch_id]               BIGINT          NOT NULL,
    [dv_r_load_source_id]       BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL,
    [dv_hash]                   CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_daily_sum_pay]
    ON [dbo].[s_spabiz_daily_sum_pay]([bk_hash] ASC, [s_spabiz_daily_sum_pay_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_daily_sum_pay]([dv_batch_id] ASC);

