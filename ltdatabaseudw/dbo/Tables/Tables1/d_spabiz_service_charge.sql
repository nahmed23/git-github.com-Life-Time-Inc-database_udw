CREATE TABLE [dbo].[d_spabiz_service_charge] (
    [d_spabiz_service_charge_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [dim_spabiz_service_charge_type_key]      CHAR (32)       NULL,
    [service_charge_id]                       BIGINT          NULL,
    [store_number]                            BIGINT          NULL,
    [deleted_date_time]                       DATETIME        NULL,
    [deleted_flag]                            CHAR (1)        NULL,
    [dim_spabiz_staff_key]                    CHAR (32)       NULL,
    [edit_date_time]                          DATETIME        NULL,
    [enabled_flag]                            CHAR (1)        NULL,
    [pay_commission_flag]                     CHAR (1)        NULL,
    [quick_id]                                VARCHAR (150)   NULL,
    [service_charge_amount]                   DECIMAL (26, 6) NULL,
    [service_charge_computed_by_percent_flag] CHAR (1)        NULL,
    [service_charge_display_name]             VARCHAR (150)   NULL,
    [service_charge_name]                     VARCHAR (300)   NULL,
    [service_charge_percent]                  DECIMAL (26, 6) NULL,
    [taxable_flag]                            CHAR (1)        NULL,
    [p_spabiz_service_charge_id]              BIGINT          NOT NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_service_charge]([dv_batch_id] ASC);

