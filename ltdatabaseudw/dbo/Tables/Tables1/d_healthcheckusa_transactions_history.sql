CREATE TABLE [dbo].[d_healthcheckusa_transactions_history] (
    [d_healthcheckusa_transactions_history_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [d_healthcheckusa_transactions_bk_hash]    CHAR (32)       NULL,
    [order_number]                             INT             NULL,
    [product_sku]                              INT             NULL,
    [effective_date_time]                      DATETIME        NULL,
    [expiration_date_time]                     DATETIME        NULL,
    [dim_employee_key]                         CHAR (32)       NULL,
    [employee_id]                              INT             NULL,
    [gl_club_id]                               INT             NULL,
    [item_amount]                              DECIMAL (26, 6) NULL,
    [item_discount]                            DECIMAL (26, 6) NULL,
    [order_for_employee_flag]                  CHAR (1)        NULL,
    [quantity]                                 INT             NULL,
    [transaction_date]                         DATETIME        NULL,
    [transaction_type]                         VARCHAR (100)   NULL,
    [p_healthcheckusa_transactions_id]         BIGINT          NOT NULL,
    [deleted_flag]                             INT             NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_healthcheckusa_transactions_history]([dv_batch_id] ASC);

