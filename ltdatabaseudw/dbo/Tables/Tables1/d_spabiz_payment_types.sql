CREATE TABLE [dbo].[d_spabiz_payment_types] (
    [d_spabiz_payment_types_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [dim_spabiz_payment_type_key] CHAR (32)       NULL,
    [payment_type_id]             DECIMAL (26, 6) NULL,
    [store_number]                DECIMAL (26, 6) NULL,
    [bank_depositable_flag]       CHAR (1)        NULL,
    [created_date_time]           DATETIME        NULL,
    [deleted_date_time]           DATETIME        NULL,
    [deleted_flag]                CHAR (1)        NULL,
    [dim_spabiz_store_key]        CHAR (32)       NULL,
    [edit_date_time]              DATETIME        NULL,
    [enabled_flag]                CHAR (1)        NULL,
    [name]                        VARCHAR (150)   NULL,
    [non_revenue_flag]            CHAR (1)        NULL,
    [pop_drawer_flag]             CHAR (1)        NULL,
    [sort_order]                  DECIMAL (26, 6) NULL,
    [verify_credit_card_flag]     CHAR (1)        NULL,
    [p_spabiz_payment_types_id]   BIGINT          NOT NULL,
    [dv_load_date_time]           DATETIME        NULL,
    [dv_load_end_date_time]       DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_payment_types]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_payment_types]([dv_batch_id]);

