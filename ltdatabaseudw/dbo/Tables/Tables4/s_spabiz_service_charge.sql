CREATE TABLE [dbo].[s_spabiz_service_charge] (
    [s_spabiz_service_charge_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [service_charge_id]          DECIMAL (26, 6) NULL,
    [counter_id]                 DECIMAL (26, 6) NULL,
    [edit_time]                  DATETIME        NULL,
    [service_charge_delete]      DECIMAL (26, 6) NULL,
    [delete_date]                DATETIME        NULL,
    [name]                       VARCHAR (300)   NULL,
    [display_name]               VARCHAR (150)   NULL,
    [store_number]               DECIMAL (26, 6) NULL,
    [pay_commission]             DECIMAL (26, 6) NULL,
    [enabled]                    DECIMAL (26, 6) NULL,
    [enabled_text]               VARCHAR (30)    NULL,
    [taxable]                    DECIMAL (26, 6) NULL,
    [amount]                     DECIMAL (26, 6) NULL,
    [dollar_percent]             DECIMAL (26, 6) NULL,
    [computed_on]                DECIMAL (26, 6) NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL,
    [dv_r_load_source_id]        BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_hash]                    CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_service_charge]
    ON [dbo].[s_spabiz_service_charge]([bk_hash] ASC, [s_spabiz_service_charge_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_service_charge]([dv_batch_id] ASC);

