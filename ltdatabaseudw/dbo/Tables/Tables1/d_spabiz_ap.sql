CREATE TABLE [dbo].[d_spabiz_ap] (
    [d_spabiz_ap_id]                      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [fact_spabiz_appointment_key]         CHAR (32)      NULL,
    [appointment_id]                      BIGINT         NULL,
    [store_number]                        BIGINT         NULL,
    [appointment_date_time]               DATETIME       NULL,
    [appointment_dim_date_key]            VARCHAR (32)   NULL,
    [appointment_dim_time_key]            VARCHAR (32)   NULL,
    [appointment_start_date_time]         DATETIME       NULL,
    [booked_by_dim_spabiz_staff_key]      CHAR (32)      NULL,
    [checkin_date_time]                   DATETIME       NULL,
    [checkin_dim_date_key]                VARCHAR (32)   NULL,
    [checkin_dim_time_key]                VARCHAR (32)   NULL,
    [confirmed_by_dim_spabiz_staff_key]   CHAR (32)      NULL,
    [created_date_time]                   DATETIME       NULL,
    [deleted_flag]                        CHAR (1)       NULL,
    [dim_spabiz_customer_key]             CHAR (32)      NULL,
    [dim_spabiz_staff_key]                CHAR (32)      NULL,
    [dim_spabiz_store_key]                CHAR (32)      NULL,
    [edit_date_time]                      DATETIME       NULL,
    [fact_spabiz_ticket_key]              CHAR (32)      NULL,
    [l_spabiz_ap_previous_status]         BIGINT         NULL,
    [l_spabiz_ap_status]                  BIGINT         NULL,
    [late_flag]                           CHAR (1)       NULL,
    [memo]                                VARCHAR (3000) NULL,
    [no_show_flag]                        CHAR (1)       NULL,
    [previous_status_dim_description_key] VARCHAR (50)   NULL,
    [previous_status_id]                  VARCHAR (50)   NULL,
    [standing_appointment_flag]           CHAR (1)       NULL,
    [status_dim_description_key]          VARCHAR (50)   NULL,
    [status_id]                           VARCHAR (50)   NULL,
    [l_spabiz_ap_book_staff_id]           BIGINT         NULL,
    [l_spabiz_ap_confirm_id]              BIGINT         NULL,
    [l_spabiz_ap_cust_id]                 BIGINT         NULL,
    [l_spabiz_ap_staff_id]                BIGINT         NULL,
    [l_spabiz_ap_ticket_id]               BIGINT         NULL,
    [p_spabiz_ap_id]                      BIGINT         NOT NULL,
    [dv_load_date_time]                   DATETIME       NULL,
    [dv_load_end_date_time]               DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ap]([dv_batch_id] ASC);

