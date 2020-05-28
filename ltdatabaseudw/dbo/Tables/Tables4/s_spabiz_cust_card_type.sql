CREATE TABLE [dbo].[s_spabiz_cust_card_type] (
    [s_spabiz_cust_card_type_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [cust_card_type_id]          DECIMAL (26, 6) NULL,
    [counter_id]                 DECIMAL (26, 6) NULL,
    [edit_time]                  DATETIME        NULL,
    [cust_card_type_delete]      DECIMAL (26, 6) NULL,
    [delete_date]                DATETIME        NULL,
    [name]                       VARCHAR (150)   NULL,
    [retail_price]               DECIMAL (26, 6) NULL,
    [days_good_for]              DECIMAL (26, 6) NULL,
    [serial_num_counter]         DECIMAL (26, 6) NULL,
    [store_number]               DECIMAL (26, 6) NULL,
    [payment_interval]           DECIMAL (26, 6) NULL,
    [service_disc]               VARCHAR (30)    NULL,
    [prod_disc]                  VARCHAR (30)    NULL,
    [disp_color]                 VARCHAR (60)    NULL,
    [initial_price]              DECIMAL (26, 6) NULL,
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
CREATE CLUSTERED INDEX [ci_s_spabiz_cust_card_type]
    ON [dbo].[s_spabiz_cust_card_type]([bk_hash] ASC, [s_spabiz_cust_card_type_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_cust_card_type]([dv_batch_id] ASC);

