CREATE TABLE [dbo].[s_exerp_inventory_transaction_log] (
    [s_exerp_inventory_transaction_log_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [inventory_transaction_log_id]         INT             NULL,
    [inventory_name]                       VARCHAR (4000)  NULL,
    [type]                                 VARCHAR (4000)  NULL,
    [comment]                              VARCHAR (4000)  NULL,
    [book_datetime]                        DATETIME        NULL,
    [quantity]                             INT             NULL,
    [unit_value]                           DECIMAL (26, 6) NULL,
    [balance_quantity]                     INT             NULL,
    [balance_value]                        DECIMAL (26, 6) NULL,
    [ets]                                  BIGINT          NULL,
    [dummy_modified_date_time]             DATETIME        NULL,
    [dv_load_date_time]                    DATETIME        NOT NULL,
    [dv_r_load_source_id]                  BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL,
    [dv_hash]                              CHAR (32)       NOT NULL,
    [dv_deleted]                           BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                          BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

