CREATE TABLE [dbo].[s_spabiz_service] (
    [s_spabiz_service_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [service_id]            DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [service_delete]        DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (150)   NULL,
    [quick_id]              VARCHAR (90)    NULL,
    [book_name]             VARCHAR (150)   NULL,
    [retail_price]          DECIMAL (26, 6) NULL,
    [date_created]          DATETIME        NULL,
    [active]                DECIMAL (26, 6) NULL,
    [cost]                  DECIMAL (26, 6) NULL,
    [time]                  VARCHAR (15)    NULL,
    [process]               VARCHAR (15)    NULL,
    [finish]                VARCHAR (15)    NULL,
    [search_cat]            DECIMAL (26, 6) NULL,
    [cost_type]             DECIMAL (26, 6) NULL,
    [call_after_x_days]     DECIMAL (26, 6) NULL,
    [pay_comish]            DECIMAL (26, 6) NULL,
    [description]           VARCHAR (3000)  NULL,
    [popup]                 VARCHAR (765)   NULL,
    [taxable]               DECIMAL (26, 6) NULL,
    [resource_count]        DECIMAL (26, 6) NULL,
    [new_extra_time]        VARCHAR (30)    NULL,
    [require_staff]         DECIMAL (26, 6) NULL,
    [date]                  DATETIME        NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [pay_tip]               DECIMAL (26, 6) NULL,
    [tip]                   DECIMAL (26, 6) NULL,
    [web_book]              DECIMAL (26, 6) NULL,
    [new_id]                DECIMAL (26, 6) NULL,
    [service_backup_id]     DECIMAL (26, 6) NULL,
    [web_view]              DECIMAL (26, 6) NULL,
    [hair_length]           VARCHAR (30)    NULL,
    [is_hilite_procedure]   DECIMAL (26, 6) NULL,
    [is_color_balance]      DECIMAL (26, 6) NULL,
    [service_level]         DECIMAL (26, 6) NULL,
    [service_class]         VARCHAR (9)     NULL,
    [allow_power_booking]   DECIMAL (26, 6) NULL,
    [req_cc]                DECIMAL (26, 6) NULL,
    [exclude_appt_gaur]     DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_service]
    ON [dbo].[s_spabiz_service]([bk_hash] ASC, [s_spabiz_service_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_service]([dv_batch_id] ASC);

