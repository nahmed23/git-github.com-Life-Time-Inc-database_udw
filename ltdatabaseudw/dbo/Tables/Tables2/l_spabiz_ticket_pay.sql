CREATE TABLE [dbo].[l_spabiz_ticket_pay] (
    [l_spabiz_ticket_pay_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [ticket_pay_id]          DECIMAL (26, 6) NULL,
    [store_id]               DECIMAL (26, 6) NULL,
    [ticket_id]              DECIMAL (26, 6) NULL,
    [pay_id]                 DECIMAL (26, 6) NULL,
    [pay_type]               DECIMAL (26, 6) NULL,
    [cust_id]                DECIMAL (26, 6) NULL,
    [shift_id]               DECIMAL (26, 6) NULL,
    [day_id]                 DECIMAL (26, 6) NULL,
    [period_id]              DECIMAL (26, 6) NULL,
    [ref_id]                 DECIMAL (26, 6) NULL,
    [staff_id]               DECIMAL (26, 6) NULL,
    [store_number]           DECIMAL (26, 6) NULL,
    [gl_account]             VARCHAR (45)    NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_batch_id]            BIGINT          NOT NULL,
    [dv_r_load_source_id]    BIGINT          NOT NULL,
    [dv_inserted_date_time]  DATETIME        NOT NULL,
    [dv_insert_user]         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_hash]                CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_spabiz_ticket_pay]
    ON [dbo].[l_spabiz_ticket_pay]([bk_hash] ASC, [l_spabiz_ticket_pay_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_spabiz_ticket_pay]([dv_batch_id] ASC);

