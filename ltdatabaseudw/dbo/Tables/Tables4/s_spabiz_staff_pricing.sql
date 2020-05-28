CREATE TABLE [dbo].[s_spabiz_staff_pricing] (
    [s_spabiz_staff_pricing_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [staff_pricing_id]          DECIMAL (26, 6) NULL,
    [counter_id]                DECIMAL (26, 6) NULL,
    [edit_time]                 DATETIME        NULL,
    [staff_service_index]       VARCHAR (150)   NULL,
    [use_price_special]         DECIMAL (26, 6) NULL,
    [retail_price]              DECIMAL (26, 6) NULL,
    [cost]                      DECIMAL (26, 6) NULL,
    [use_time_special]          DECIMAL (26, 6) NULL,
    [time]                      VARCHAR (30)    NULL,
    [process]                   VARCHAR (30)    NULL,
    [finish]                    VARCHAR (30)    NULL,
    [new_extra_time]            VARCHAR (30)    NULL,
    [sales_service_total]       DECIMAL (26, 6) NULL,
    [store_number]              DECIMAL (26, 6) NULL,
    [dv_load_date_time]         DATETIME        NOT NULL,
    [dv_r_load_source_id]       BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL,
    [dv_hash]                   CHAR (32)       NOT NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_staff_pricing]
    ON [dbo].[s_spabiz_staff_pricing]([bk_hash] ASC, [s_spabiz_staff_pricing_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_staff_pricing]([dv_batch_id] ASC);

