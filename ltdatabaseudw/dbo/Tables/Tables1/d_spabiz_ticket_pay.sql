CREATE TABLE [dbo].[d_spabiz_ticket_pay] (
    [d_spabiz_ticket_pay_id]                     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)       NOT NULL,
    [fact_spabiz_ticket_payment_key]             CHAR (32)       NULL,
    [ticket_pay_id]                              BIGINT          NULL,
    [store_number]                               BIGINT          NULL,
    [created_date_time]                          DATETIME        NULL,
    [dim_spabiz_customer_key]                    CHAR (32)       NULL,
    [dim_spabiz_payment_type_key]                CHAR (32)       NULL,
    [dim_spabiz_shift_key]                       CHAR (32)       NULL,
    [dim_spabiz_store_key]                       CHAR (32)       NULL,
    [edit_date_time]                             DATETIME        NULL,
    [fact_spabiz_ticket_key]                     CHAR (32)       NULL,
    [hash_for_reference_record_for_payment_type] CHAR (32)       NULL,
    [payment_amount]                             DECIMAL (26, 6) NULL,
    [payment_checked_during_close]               CHAR (1)        NULL,
    [payment_number]                             VARCHAR (50)    NULL,
    [payment_status_dim_description_key]         VARCHAR (50)    NULL,
    [payment_status_id]                          VARCHAR (50)    NULL,
    [l_spabiz_ticket_pay_cust_id]                BIGINT          NULL,
    [l_spabiz_ticket_pay_pay_id]                 BIGINT          NULL,
    [l_spabiz_ticket_pay_ref_id]                 BIGINT          NULL,
    [l_spabiz_ticket_pay_shift_id]               BIGINT          NULL,
    [l_spabiz_ticket_pay_ticket_id]              BIGINT          NULL,
    [p_spabiz_ticket_pay_id]                     BIGINT          NOT NULL,
    [dv_load_date_time]                          DATETIME        NULL,
    [dv_load_end_date_time]                      DATETIME        NULL,
    [dv_batch_id]                                BIGINT          NOT NULL,
    [dv_inserted_date_time]                      DATETIME        NOT NULL,
    [dv_insert_user]                             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                       DATETIME        NULL,
    [dv_update_user]                             VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ticket_pay]([dv_batch_id] ASC);

