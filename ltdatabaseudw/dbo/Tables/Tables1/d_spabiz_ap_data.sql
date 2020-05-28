CREATE TABLE [dbo].[d_spabiz_ap_data] (
    [d_spabiz_ap_data_id]                      BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)     NOT NULL,
    [fact_spabiz_appointment_item_key]         CHAR (32)     NULL,
    [ap_data_id]                               BIGINT        NULL,
    [store_number]                             BIGINT        NULL,
    [booked_by_dim_staff_key]                  CHAR (32)     NULL,
    [booked_on_web_flag]                       CHAR (1)      NULL,
    [check_in_date_time]                       DATETIME      NULL,
    [check_out_date_time]                      DATETIME      NULL,
    [data_type_dim_description_key]            VARCHAR (50)  NULL,
    [data_type_id]                             BIGINT        NULL,
    [deleted_flag]                             CHAR (1)      NULL,
    [dim_spabiz_block_time_key]                CHAR (32)     NULL,
    [dim_spabiz_customer_key]                  CHAR (32)     NULL,
    [dim_spabiz_service_key]                   CHAR (32)     NULL,
    [dim_spabiz_store_key]                     CHAR (32)     NULL,
    [edit_date_time]                           DATETIME      NULL,
    [end_date_time]                            DATETIME      NULL,
    [fact_spabiz_appointment_key]              CHAR (32)     NULL,
    [fact_spabiz_ticket_item_key]              CHAR (32)     NULL,
    [note]                                     VARCHAR (150) NULL,
    [parent_fact_spabiz_appointment_item_key]  CHAR (32)     NULL,
    [related_fact_spabiz_appointment_item_key] CHAR (32)     NULL,
    [resource_block_flag]                      CHAR (1)      NULL,
    [resource_dim_spabiz_staff_key]            CHAR (32)     NULL,
    [service_dim_spabiz_staff_key]             CHAR (32)     NULL,
    [standing_appointment_flag]                CHAR (1)      NULL,
    [start_date_time]                          DATETIME      NULL,
    [status_dim_description_key]               VARCHAR (50)  NULL,
    [status_id]                                INT           NULL,
    [s_spabiz_ap_data_data_type]               BIGINT        NULL,
    [s_spabiz_ap_data_status]                  BIGINT        NULL,
    [p_spabiz_ap_data_id]                      BIGINT        NOT NULL,
    [dv_load_date_time]                        DATETIME      NULL,
    [dv_load_end_date_time]                    DATETIME      NULL,
    [dv_batch_id]                              BIGINT        NOT NULL,
    [dv_inserted_date_time]                    DATETIME      NOT NULL,
    [dv_insert_user]                           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                     DATETIME      NULL,
    [dv_update_user]                           VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ap_data]([dv_batch_id] ASC);

