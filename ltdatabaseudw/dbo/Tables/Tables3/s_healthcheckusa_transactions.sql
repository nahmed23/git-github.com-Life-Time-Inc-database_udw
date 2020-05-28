CREATE TABLE [dbo].[s_healthcheckusa_transactions] (
    [s_healthcheckusa_transactions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [order_number]                     INT             NULL,
    [sku]                              INT             NULL,
    [transaction_type]                 VARCHAR (100)   NULL,
    [transaction_date]                 DATETIME        NULL,
    [quantity]                         INT             NULL,
    [item_amount]                      DECIMAL (26, 6) NULL,
    [item_discount]                    DECIMAL (26, 6) NULL,
    [order_for_employee_flag]          CHAR (1)        NULL,
    [dummy_modified_date_time]         DATETIME        NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_r_load_source_id]              BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_hash]                          CHAR (32)       NOT NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_healthcheckusa_transactions]([dv_batch_id] ASC);

