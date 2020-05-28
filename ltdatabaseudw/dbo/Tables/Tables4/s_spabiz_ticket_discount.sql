CREATE TABLE [dbo].[s_spabiz_ticket_discount] (
    [s_spabiz_ticket_discount_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [ticket_discount_id]          DECIMAL (26, 6) NULL,
    [counter_id]                  DECIMAL (26, 6) NULL,
    [edit_time]                   DATETIME        NULL,
    [date]                        DATETIME        NULL,
    [amount]                      DECIMAL (26, 6) NULL,
    [ticket_discount_percent]     DECIMAL (26, 6) NULL,
    [status]                      DECIMAL (26, 6) NULL,
    [double_it]                   DECIMAL (26, 6) NULL,
    [store_number]                DECIMAL (26, 6) NULL,
    [created_by_staff]            DECIMAL (26, 6) NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_ticket_discount]
    ON [dbo].[s_spabiz_ticket_discount]([bk_hash] ASC, [s_spabiz_ticket_discount_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ticket_discount]([dv_batch_id] ASC);

