CREATE TABLE [dbo].[s_spabiz_ap_data] (
    [s_spabiz_ap_data_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [ap_data_id]              DECIMAL (26, 6) NULL,
    [counter_id]              DECIMAL (26, 6) NULL,
    [edit_time]               DATETIME        NULL,
    [data_type]               DECIMAL (26, 6) NULL,
    [s_time]                  VARCHAR (150)   NULL,
    [e_time]                  VARCHAR (150)   NULL,
    [start_time]              DATETIME        NULL,
    [end_time]                DATETIME        NULL,
    [time]                    VARCHAR (15)    NULL,
    [ap_time_index]           VARCHAR (150)   NULL,
    [status]                  DECIMAL (26, 6) NULL,
    [note]                    VARCHAR (150)   NULL,
    [price]                   DECIMAL (26, 6) NULL,
    [ap_data_delete]          DECIMAL (26, 6) NULL,
    [ress_time]               VARCHAR (150)   NULL,
    [rese_time]               VARCHAR (150)   NULL,
    [check_in]                DATETIME        NULL,
    [check_out]               DATETIME        NULL,
    [block_time_name]         VARCHAR (150)   NULL,
    [retention_name]          VARCHAR (150)   NULL,
    [retention_color]         DECIMAL (26, 6) NULL,
    [customer_first_name]     VARCHAR (150)   NULL,
    [customer_last_name]      VARCHAR (150)   NULL,
    [customer_service_visits] DECIMAL (26, 6) NULL,
    [service_book_name]       VARCHAR (150)   NULL,
    [service_name]            VARCHAR (150)   NULL,
    [store_number]            DECIMAL (26, 6) NULL,
    [standing]                DECIMAL (26, 6) NULL,
    [res_block]               DECIMAL (26, 6) NULL,
    [booked_on_web]           DECIMAL (26, 6) NULL,
    [activity_id]             DECIMAL (26, 6) NULL,
    [service_ap_id]           DECIMAL (26, 6) NULL,
    [htng_id]                 VARCHAR (150)   NULL,
    [demand_force]            DECIMAL (26, 6) NULL,
    [dv_load_date_time]       DATETIME        NOT NULL,
    [dv_r_load_source_id]     BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL,
    [dv_hash]                 CHAR (32)       NOT NULL,
    [dv_batch_id]             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_ap_data]
    ON [dbo].[s_spabiz_ap_data]([bk_hash] ASC, [s_spabiz_ap_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ap_data]([dv_batch_id] ASC);

