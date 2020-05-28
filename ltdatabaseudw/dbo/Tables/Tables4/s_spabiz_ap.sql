CREATE TABLE [dbo].[s_spabiz_ap] (
    [s_spabiz_ap_id]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [ap_id]                 DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [date]                  DATETIME        NULL,
    [date_cust]             VARCHAR (150)   NULL,
    [status]                DECIMAL (26, 6) NULL,
    [date_status]           VARCHAR (150)   NULL,
    [checkin_time]          DATETIME        NULL,
    [start_time]            DATETIME        NULL,
    [late]                  DECIMAL (26, 6) NULL,
    [status_old]            DECIMAL (26, 6) NULL,
    [book_time]             DATETIME        NULL,
    [memo]                  VARCHAR (3000)  NULL,
    [ap_delete]             DECIMAL (26, 6) NULL,
    [time_id]               DECIMAL (26, 6) NULL,
    [standing]              DECIMAL (26, 6) NULL,
    [alt_cust_id]           VARCHAR (18)    NULL,
    [alt_service_id]        VARCHAR (150)   NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [start_booking]         DATETIME        NULL,
    [stop_booking]          DATETIME        NULL,
    [upsell]                DECIMAL (26, 6) NULL,
    [activity_id]           DECIMAL (26, 6) NULL,
    [no_show]               DECIMAL (26, 6) NULL,
    [appointment_type]      DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_ap]
    ON [dbo].[s_spabiz_ap]([bk_hash] ASC, [s_spabiz_ap_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ap]([dv_batch_id] ASC);

