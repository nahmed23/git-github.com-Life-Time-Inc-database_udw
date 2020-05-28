CREATE TABLE [dbo].[s_spabiz_payment_types] (
    [s_spabiz_payment_types_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [payment_types_id]          DECIMAL (26, 6) NULL,
    [edit_time]                 DATETIME        NULL,
    [payment_types_delete]      DECIMAL (26, 6) NULL,
    [delete_date]               DATETIME        NULL,
    [name]                      VARCHAR (150)   NULL,
    [quick_id]                  VARCHAR (150)   NULL,
    [pay_type]                  DECIMAL (26, 6) NULL,
    [enabled]                   DECIMAL (26, 6) NULL,
    [depositable]               DECIMAL (26, 6) NULL,
    [service_charge]            DECIMAL (26, 6) NULL,
    [programmed]                DECIMAL (26, 6) NULL,
    [icon]                      VARCHAR (150)   NULL,
    [quick_key]                 DECIMAL (26, 6) NULL,
    [non_revenue]               DECIMAL (26, 6) NULL,
    [verify]                    DECIMAL (26, 6) NULL,
    [date_time]                 DATETIME        NULL,
    [store_number]              DECIMAL (26, 6) NULL,
    [pop_drawer]                DECIMAL (26, 6) NULL,
    [multi_copy]                DECIMAL (26, 6) NULL,
    [signature_line]            DECIMAL (26, 6) NULL,
    [default_room_number]       VARCHAR (150)   NULL,
    [hotel_post]                DECIMAL (26, 6) NULL,
    [hotel_pay_code]            VARCHAR (15)    NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_payment_types]
    ON [dbo].[s_spabiz_payment_types]([bk_hash] ASC, [s_spabiz_payment_types_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_payment_types]([dv_batch_id] ASC);

