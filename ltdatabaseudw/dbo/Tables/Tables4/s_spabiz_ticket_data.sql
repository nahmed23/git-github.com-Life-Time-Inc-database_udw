CREATE TABLE [dbo].[s_spabiz_ticket_data] (
    [s_spabiz_ticket_data_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)       NOT NULL,
    [ticket_data_id]          DECIMAL (26, 6) NULL,
    [edit_time]               DATETIME        NULL,
    [line_num]                DECIMAL (26, 6) NULL,
    [qty]                     DECIMAL (26, 6) NULL,
    [retail_price]            DECIMAL (26, 6) NULL,
    [cost]                    DECIMAL (26, 6) NULL,
    [discount_amount]         DECIMAL (26, 6) NULL,
    [discount_per]            DECIMAL (26, 6) NULL,
    [date]                    DATETIME        NULL,
    [status]                  DECIMAL (26, 6) NULL,
    [return_where]            DECIMAL (26, 6) NULL,
    [ext_price]               DECIMAL (26, 6) NULL,
    [ticket_dis_amt]          DECIMAL (26, 6) NULL,
    [taxable]                 DECIMAL (26, 6) NULL,
    [service_amt]             DECIMAL (26, 6) NULL,
    [service_qty]             DECIMAL (26, 6) NULL,
    [product_amt]             DECIMAL (26, 6) NULL,
    [product_qty]             DECIMAL (26, 6) NULL,
    [other_amt]               DECIMAL (26, 6) NULL,
    [other_qty]               DECIMAL (26, 6) NULL,
    [store_number]            DECIMAL (26, 6) NULL,
    [start_time]              DATETIME        NULL,
    [end_time]                DATETIME        NULL,
    [merged_item]             DECIMAL (26, 6) NULL,
    [ship_to]                 VARCHAR (1500)  NULL,
    [ship_status]             DECIMAL (26, 6) NULL,
    [dv_load_date_time]       DATETIME        NOT NULL,
    [dv_batch_id]             BIGINT          NOT NULL,
    [dv_r_load_source_id]     BIGINT          NOT NULL,
    [dv_inserted_date_time]   DATETIME        NOT NULL,
    [dv_insert_user]          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]    DATETIME        NULL,
    [dv_update_user]          VARCHAR (50)    NULL,
    [dv_hash]                 CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_ticket_data]
    ON [dbo].[s_spabiz_ticket_data]([bk_hash] ASC, [s_spabiz_ticket_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_ticket_data]([dv_batch_id] ASC);

