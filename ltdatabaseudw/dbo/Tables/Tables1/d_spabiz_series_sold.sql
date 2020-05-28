CREATE TABLE [dbo].[d_spabiz_series_sold] (
    [d_spabiz_series_sold_id]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [fact_spabiz_series_sold_key]        CHAR (32)       NULL,
    [series_sold_id]                     BIGINT          NULL,
    [store_number]                       BIGINT          NULL,
    [balance]                            DECIMAL (26, 6) NULL,
    [created_date_time]                  DATETIME        NULL,
    [dim_spabiz_customer_key]            CHAR (32)       NULL,
    [dim_spabiz_series_key]              CHAR (32)       NULL,
    [dim_spabiz_store_key]               CHAR (32)       NULL,
    [edit_date_time]                     DATETIME        NULL,
    [fact_spabiz_ticket_key]             CHAR (32)       NULL,
    [first_dim_spabiz_staff_key]         CHAR (32)       NULL,
    [last_used_date_time]                DATETIME        NULL,
    [purchasing_dim_spabiz_customer_key] CHAR (32)       NULL,
    [retail_price]                       DECIMAL (26, 6) NULL,
    [second_dim_spabiz_staff_key]        CHAR (32)       NULL,
    [status_dim_description_key]         VARCHAR (50)    NULL,
    [status_id]                          VARCHAR (50)    NULL,
    [l_spabiz_series_sold_buy_cust_id]   BIGINT          NULL,
    [l_spabiz_series_sold_cust_id]       BIGINT          NULL,
    [l_spabiz_series_sold_series_id]     BIGINT          NULL,
    [l_spabiz_series_sold_staff_id_1]    BIGINT          NULL,
    [l_spabiz_series_sold_staff_id_2]    BIGINT          NULL,
    [l_spabiz_series_sold_ticket_id]     BIGINT          NULL,
    [s_spabiz_series_sold_status]        DECIMAL (26, 6) NULL,
    [p_spabiz_series_sold_id]            BIGINT          NOT NULL,
    [dv_load_date_time]                  DATETIME        NULL,
    [dv_load_end_date_time]              DATETIME        NULL,
    [dv_batch_id]                        BIGINT          NOT NULL,
    [dv_inserted_date_time]              DATETIME        NOT NULL,
    [dv_insert_user]                     VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]               DATETIME        NULL,
    [dv_update_user]                     VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_series_sold]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_series_sold]([dv_batch_id]);

