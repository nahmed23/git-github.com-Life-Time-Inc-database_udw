CREATE TABLE [dbo].[s_spabiz_ticket] (
    [s_spabiz_ticket_id]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [ticket_id]                  DECIMAL (26, 6) NULL,
    [store_number]               DECIMAL (26, 6) NULL,
    [edit_time]                  DATETIME        NULL,
    [ticket_num]                 VARCHAR (150)   NULL,
    [ticket_id_for_day]          VARCHAR (15)    NULL,
    [date]                       DATETIME        NULL,
    [status]                     DECIMAL (26, 6) NULL,
    [sales_subtotal]             DECIMAL (26, 6) NULL,
    [sales_total]                DECIMAL (26, 6) NULL,
    [sales_product_total]        DECIMAL (26, 6) NULL,
    [sales_service_total]        DECIMAL (26, 6) NULL,
    [cash_change]                DECIMAL (26, 6) NULL,
    [discount_total]             DECIMAL (26, 6) NULL,
    [discount_service]           DECIMAL (26, 6) NULL,
    [discount_product]           DECIMAL (26, 6) NULL,
    [tax_total]                  DECIMAL (26, 6) NULL,
    [check_in_time]              DATETIME        NULL,
    [late]                       DECIMAL (26, 6) NULL,
    [tip]                        DECIMAL (26, 6) NULL,
    [used_image_script]          DECIMAL (26, 6) NULL,
    [cust_balance]               DECIMAL (26, 6) NULL,
    [time]                       DATETIME        NULL,
    [check_in_status]            DECIMAL (26, 6) NULL,
    [discount_dbl]               DECIMAL (26, 6) NULL,
    [performed_value_added_serv] DECIMAL (26, 6) NULL,
    [product_only]               DECIMAL (26, 6) NULL,
    [service_only]               DECIMAL (26, 6) NULL,
    [has_product]                DECIMAL (26, 6) NULL,
    [has_service]                DECIMAL (26, 6) NULL,
    [has_only_product]           DECIMAL (26, 6) NULL,
    [has_only_service]           DECIMAL (26, 6) NULL,
    [has_both]                   DECIMAL (26, 6) NULL,
    [sales_gift_total]           DECIMAL (26, 6) NULL,
    [sales_package_total]        DECIMAL (26, 6) NULL,
    [sales_series_total]         DECIMAL (26, 6) NULL,
    [note]                       VARCHAR (1500)  NULL,
    [pro_message_answer_2]       DECIMAL (26, 6) NULL,
    [pro_message_3]              VARCHAR (300)   NULL,
    [pro_message_answer_3]       DECIMAL (26, 6) NULL,
    [pro_message]                VARCHAR (300)   NULL,
    [pro_message_1]              VARCHAR (300)   NULL,
    [pro_message_answer_1]       DECIMAL (26, 6) NULL,
    [pro_message_2]              VARCHAR (300)   NULL,
    [is_master_ticket]           DECIMAL (26, 6) NULL,
    [pay_counter]                DECIMAL (26, 6) NULL,
    [processed_on]               DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_ticket]
    ON [dbo].[s_spabiz_ticket]([bk_hash] ASC, [s_spabiz_ticket_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ticket]([dv_batch_id] ASC);

