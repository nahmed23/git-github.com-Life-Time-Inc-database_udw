CREATE TABLE [dbo].[d_spabiz_gift_certificate] (
    [d_spabiz_gift_certificate_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [fact_spabiz_gift_certificate_key]      CHAR (32)       NULL,
    [gift_certificate_id]                   BIGINT          NULL,
    [store_number]                          BIGINT          NULL,
    [created_date_time]                     DATETIME        NULL,
    [dim_spabiz_customer_key]               CHAR (32)       NULL,
    [dim_spabiz_gift_certificate_type_key]  CHAR (32)       NULL,
    [dim_spabiz_store_key]                  CHAR (32)       NULL,
    [edit_date_time]                        DATETIME        NULL,
    [fact_spabiz_ticket_key]                CHAR (32)       NULL,
    [first_dim_spabiz_staff_key]            CHAR (32)       NULL,
    [gift_certificate_amount]               DECIMAL (26, 6) NULL,
    [gift_certificate_balance]              DECIMAL (26, 6) NULL,
    [purchasing_dim_spabiz_customer_key]    CHAR (32)       NULL,
    [second_dim_spabiz_staff_key]           CHAR (32)       NULL,
    [selling_amount]                        DECIMAL (26, 6) NULL,
    [serial_number]                         VARCHAR (50)    NULL,
    [status_description_id]                 VARCHAR (50)    NULL,
    [status_dim_description_key]            VARCHAR (50)    NULL,
    [status_id]                             BIGINT          NULL,
    [l_spabiz_gift_certificate_buy_cust_id] BIGINT          NULL,
    [l_spabiz_gift_certificate_cust_id]     BIGINT          NULL,
    [l_spabiz_gift_certificate_gift_id]     BIGINT          NULL,
    [l_spabiz_gift_certificate_staff_id_1]  BIGINT          NULL,
    [l_spabiz_gift_certificate_staff_id_2]  BIGINT          NULL,
    [l_spabiz_gift_certificate_ticket_id]   BIGINT          NULL,
    [p_spabiz_gift_certificate_id]          BIGINT          NOT NULL,
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
    ON [dbo].[d_spabiz_gift_certificate]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_gift_certificate]([dv_batch_id]);

