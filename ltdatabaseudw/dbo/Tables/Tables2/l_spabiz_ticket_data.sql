CREATE TABLE [dbo].[l_spabiz_ticket_data] (
    [l_spabiz_ticket_data_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [ticket_data_id]           DECIMAL (26, 6) NULL,
    [counter_id]               DECIMAL (26, 6) NULL,
    [store_id]                 DECIMAL (26, 6) NULL,
    [ticket_id]                DECIMAL (26, 6) NULL,
    [item_id]                  DECIMAL (26, 6) NULL,
    [data_type]                DECIMAL (26, 6) NULL,
    [group_id]                 DECIMAL (26, 6) NULL,
    [cust_id]                  DECIMAL (26, 6) NULL,
    [staff_id_1]               DECIMAL (26, 6) NULL,
    [staff_id_2]               DECIMAL (26, 6) NULL,
    [discount_id]              DECIMAL (26, 6) NULL,
    [shift_id]                 DECIMAL (26, 6) NULL,
    [day_id]                   DECIMAL (26, 6) NULL,
    [period_id]                DECIMAL (26, 6) NULL,
    [retention]                DECIMAL (26, 6) NULL,
    [sub_group_id]             DECIMAL (26, 6) NULL,
    [package_id]               DECIMAL (26, 6) NULL,
    [time_id]                  DECIMAL (26, 6) NULL,
    [promo_id]                 DECIMAL (26, 6) NULL,
    [store_number]             DECIMAL (26, 6) NULL,
    [gl_account]               VARCHAR (45)    NULL,
    [sales_gl_account]         VARCHAR (60)    NULL,
    [discount_gl_account]      VARCHAR (60)    NULL,
    [cost_gl_account]          VARCHAR (60)    NULL,
    [master_ticket]            DECIMAL (26, 6) NULL,
    [return_reason]            DECIMAL (26, 6) NULL,
    [service_charge_parent_id] DECIMAL (26, 6) NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_batch_id]              BIGINT          NOT NULL,
    [dv_r_load_source_id]      BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_hash]                  CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_spabiz_ticket_data]
    ON [dbo].[l_spabiz_ticket_data]([bk_hash] ASC, [l_spabiz_ticket_data_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_ticket_data]([dv_batch_id] ASC);

