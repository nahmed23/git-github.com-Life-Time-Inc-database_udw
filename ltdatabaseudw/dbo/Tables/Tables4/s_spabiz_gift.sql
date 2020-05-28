CREATE TABLE [dbo].[s_spabiz_gift] (
    [s_spabiz_gift_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [gift_id]               DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [gift_delete]           DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [pay_commission]        DECIMAL (26, 6) NULL,
    [retail_price]          DECIMAL (26, 6) NULL,
    [price_changable]       DECIMAL (26, 6) NULL,
    [days_good_for]         DECIMAL (26, 6) NULL,
    [use_for]               DECIMAL (26, 6) NULL,
    [refundable]            DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_gift]
    ON [dbo].[s_spabiz_gift]([bk_hash] ASC, [s_spabiz_gift_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_gift]([dv_batch_id] ASC);

