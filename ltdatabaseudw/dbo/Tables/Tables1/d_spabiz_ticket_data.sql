CREATE TABLE [dbo].[d_spabiz_ticket_data] (
    [d_spabiz_ticket_data_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_spabiz_ticket_item_key]         CHAR (32)       NULL,
    [ticket_data_id]                      BIGINT          NULL,
    [store_number]                        BIGINT          NULL,
    [commission_amount]                   DECIMAL (26, 6) NULL,
    [commission_discount_amount]          DECIMAL (26, 6) NULL,
    [cost]                                DECIMAL (26, 6) NULL,
    [dim_spabiz_category_key]             CHAR (32)       NULL,
    [dim_spabiz_customer_key]             CHAR (32)       NULL,
    [dim_spabiz_data_type_key]            CHAR (32)       NULL,
    [dim_spabiz_discount_key]             CHAR (32)       NULL,
    [dim_spabiz_gift_certificate_key]     CHAR (32)       NULL,
    [dim_spabiz_product_key]              CHAR (32)       NULL,
    [dim_spabiz_series_key]               CHAR (32)       NULL,
    [dim_spabiz_service_key]              CHAR (32)       NULL,
    [dual_commission_flag]                CHAR (1)        NULL,
    [edit_date_time]                      DATETIME        NULL,
    [employee_commission_amount]          DECIMAL (26, 6) NULL,
    [employee_commission_discount_amount] DECIMAL (26, 6) NULL,
    [end_date_time]                       DATETIME        NULL,
    [ext_price]                           DECIMAL (26, 6) NULL,
    [fact_spabiz_ticket_key]              CHAR (32)       NULL,
    [first_dim_spabiz_staff_key]          CHAR (32)       NULL,
    [item_discount_amount]                DECIMAL (26, 6) NULL,
    [item_id_store_number_hash]           CHAR (32)       NULL,
    [line_number]                         BIGINT          NULL,
    [other_amount]                        DECIMAL (26, 6) NULL,
    [other_quantity]                      BIGINT          NULL,
    [product_amount]                      DECIMAL (26, 6) NULL,
    [product_quantity]                    BIGINT          NULL,
    [quantity]                            BIGINT          NULL,
    [retail_price]                        DECIMAL (26, 6) NULL,
    [second_dim_spabiz_staff_key]         CHAR (32)       NULL,
    [service_amount]                      DECIMAL (26, 6) NULL,
    [service_quantity]                    BIGINT          NULL,
    [service_shop_charge]                 DECIMAL (26, 6) NULL,
    [start_date_time]                     DATETIME        NULL,
    [status_dim_description_key]          VARCHAR (50)    NULL,
    [status_id]                           VARCHAR (50)    NULL,
    [sub_dim_spabiz_category_key]         CHAR (32)       NULL,
    [ticket_id]                           BIGINT          NULL,
    [ticket_item_date_time]               DATETIME        NULL,
    [ticket_total_discount_amount]        DECIMAL (26, 6) NULL,
    [l_spabiz_ticket_data_cust_id]        BIGINT          NULL,
    [l_spabiz_ticket_data_data_type]      BIGINT          NULL,
    [l_spabiz_ticket_data_day_id]         BIGINT          NULL,
    [l_spabiz_ticket_data_discount_id]    BIGINT          NULL,
    [l_spabiz_ticket_data_group_id]       BIGINT          NULL,
    [l_spabiz_ticket_data_item_id]        BIGINT          NULL,
    [l_spabiz_ticket_data_staff_id_1]     BIGINT          NULL,
    [l_spabiz_ticket_data_staff_id_2]     BIGINT          NULL,
    [l_spabiz_ticket_data_sub_group_id]   BIGINT          NULL,
    [p_spabiz_ticket_data_id]             BIGINT          NOT NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_ticket_data]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_ticket_data]([dv_batch_id]);

