﻿CREATE TABLE [dbo].[d_spabiz_ticket] (
    [d_spabiz_ticket_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [fact_spabiz_ticket_key]      CHAR (32)       NULL,
    [ticket_id]                   BIGINT          NULL,
    [store_number]                BIGINT          NULL,
    [cash_change]                 DECIMAL (26, 6) NULL,
    [check_in_date_time]          DATETIME        NULL,
    [created_date_time]           DATETIME        NULL,
    [dim_spabiz_customer_key]     CHAR (32)       NULL,
    [dim_spabiz_payment_type_key] CHAR (32)       NULL,
    [dim_spabiz_shift_key]        CHAR (32)       NULL,
    [dim_spabiz_staff_key]        CHAR (32)       NULL,
    [dim_spabiz_store_key]        CHAR (32)       NULL,
    [discount_product]            DECIMAL (26, 6) NULL,
    [discount_service]            DECIMAL (26, 6) NULL,
    [discount_total]              DECIMAL (26, 6) NULL,
    [edit_date_time]              DATETIME        NULL,
    [fact_spabiz_appointment_key] CHAR (32)       NULL,
    [is_master_ticket_flag]       CHAR (1)        NULL,
    [late]                        CHAR (1)        NULL,
    [note]                        VARCHAR (4000)  NULL,
    [sales_gift_total]            DECIMAL (26, 6) NULL,
    [sales_package_total]         DECIMAL (26, 6) NULL,
    [sales_product_total]         DECIMAL (26, 6) NULL,
    [sales_series_total]          DECIMAL (26, 6) NULL,
    [sales_service_total]         DECIMAL (26, 6) NULL,
    [sales_subtotal]              DECIMAL (26, 6) NULL,
    [sales_total]                 DECIMAL (26, 6) NULL,
    [status_dim_description_key]  VARCHAR (50)    NULL,
    [status_id]                   VARCHAR (50)    NULL,
    [tax_total]                   DECIMAL (26, 6) NULL,
    [ticket_id_for_day]           BIGINT          NULL,
    [ticket_number]               VARCHAR (150)   NULL,
    [tip_amount]                  DECIMAL (26, 6) NULL,
    [voider_dim_spabiz_staff_key] CHAR (32)       NULL,
    [l_spabiz_ticket_ap_id]       BIGINT          NULL,
    [l_spabiz_ticket_cust_id]     BIGINT          NULL,
    [l_spabiz_ticket_pay_type_id] BIGINT          NULL,
    [l_spabiz_ticket_shift_id]    BIGINT          NULL,
    [l_spabiz_ticket_staff_id]    BIGINT          NULL,
    [l_spabiz_ticket_voider_id]   BIGINT          NULL,
    [p_spabiz_ticket_id]          BIGINT          NOT NULL,
    [dv_load_date_time]           DATETIME        NULL,
    [dv_load_end_date_time]       DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ticket]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_ticket]([dv_batch_id]);

