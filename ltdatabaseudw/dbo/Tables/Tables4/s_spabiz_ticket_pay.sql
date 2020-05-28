CREATE TABLE [dbo].[s_spabiz_ticket_pay] (
    [s_spabiz_ticket_pay_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [ticket_pay_id]          DECIMAL (26, 6) NULL,
    [counter_id]             DECIMAL (26, 6) NULL,
    [edit_time]              DATETIME        NULL,
    [date]                   DATETIME        NULL,
    [pay_num]                VARCHAR (150)   NULL,
    [status]                 DECIMAL (26, 6) NULL,
    [ok]                     DECIMAL (26, 6) NULL,
    [amount]                 DECIMAL (26, 6) NULL,
    [approval]               VARCHAR (150)   NULL,
    [store_number]           DECIMAL (26, 6) NULL,
    [acq]                    VARCHAR (600)   NULL,
    [adjusted]               DECIMAL (26, 6) NULL,
    [card_is]                DECIMAL (26, 6) NULL,
    [pay_counter]            DECIMAL (26, 6) NULL,
    [process_data]           VARCHAR (600)   NULL,
    [ref_no]                 VARCHAR (150)   NULL,
    [token_1]                VARCHAR (600)   NULL,
    [token_2]                VARCHAR (600)   NULL,
    [token_3]                VARCHAR (600)   NULL,
    [sbcc]                   DECIMAL (26, 6) NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_ticket_pay]
    ON [dbo].[s_spabiz_ticket_pay]([bk_hash] ASC, [s_spabiz_ticket_pay_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ticket_pay]([dv_batch_id] ASC);

