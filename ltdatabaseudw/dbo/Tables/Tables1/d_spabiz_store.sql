CREATE TABLE [dbo].[d_spabiz_store] (
    [d_spabiz_store_id]               BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [dim_spabiz_store_key]            CHAR (32)       NULL,
    [store_number]                    DECIMAL (26, 6) NULL,
    [deleted_date_time]               DATETIME        NULL,
    [deleted_flag]                    CHAR (1)        NULL,
    [edit_date_time]                  DATETIME        NULL,
    [open_day_1_sunday_flag]          CHAR (1)        NULL,
    [open_day_2_monday_flag]          CHAR (1)        NULL,
    [open_day_3_tuesday_flag]         CHAR (1)        NULL,
    [open_day_4_wednesday_flag]       CHAR (1)        NULL,
    [open_day_5_thursday_flag]        CHAR (1)        NULL,
    [open_day_6_friday_flag]          CHAR (1)        NULL,
    [open_day_7_saturday_flag]        CHAR (1)        NULL,
    [power_booking_flag]              CHAR (1)        NULL,
    [quick_id]                        VARCHAR (30)    NULL,
    [store_address_city]              VARCHAR (60)    NULL,
    [store_address_country]           VARCHAR (150)   NULL,
    [store_address_line_1]            VARCHAR (360)   NULL,
    [store_address_line_2]            VARCHAR (360)   NULL,
    [store_address_postal_code]       VARCHAR (30)    NULL,
    [store_address_state_or_province] VARCHAR (50)    NULL,
    [store_id]                        DECIMAL (26, 6) NULL,
    [store_name]                      VARCHAR (150)   NULL,
    [store_phone_number]              VARCHAR (150)   NULL,
    [p_spabiz_store_id]               BIGINT          NOT NULL,
    [dv_load_date_time]               DATETIME        NULL,
    [dv_load_end_date_time]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_store]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_store]([dv_batch_id]);

