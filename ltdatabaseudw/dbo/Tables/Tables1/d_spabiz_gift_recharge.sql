CREATE TABLE [dbo].[d_spabiz_gift_recharge] (
    [d_spabiz_gift_recharge_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [fact_spabiz_gift_recharge_key]         CHAR (32)       NULL,
    [gift_recharge_id]                      BIGINT          NULL,
    [store_number]                          BIGINT          NULL,
    [expiration_date_time]                  DATETIME        NULL,
    [dim_spabiz_store_key]                  CHAR (32)       NULL,
    [edit_date_time]                        DATETIME        NULL,
    [fact_spabiz_gift_certificate_key]      CHAR (32)       NULL,
    [fact_spabiz_ticket_item_key]           CHAR (32)       NULL,
    [fact_spabiz_ticket_key]                CHAR (32)       NULL,
    [gift_recharge_amount]                  DECIMAL (26, 6) NULL,
    [l_spabiz_gift_recharge_gift_id]        BIGINT          NULL,
    [l_spabiz_gift_recharge_ticket_data_id] BIGINT          NULL,
    [l_spabiz_gift_recharge_ticket_id]      BIGINT          NULL,
    [p_spabiz_gift_recharge_id]             BIGINT          NOT NULL,
    [dv_load_date_time]                     DATETIME        NULL,
    [dv_load_end_date_time]                 DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_gift_recharge]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_gift_recharge]([dv_batch_id]);

