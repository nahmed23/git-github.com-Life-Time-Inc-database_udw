CREATE TABLE [dbo].[stage_hash_healthcheckusa_Transactions] (
    [stage_hash_healthcheckusa_Transactions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [OrderNumber]                               INT             NULL,
    [SKU]                                       INT             NULL,
    [TransactionType]                           VARCHAR (100)   NULL,
    [TransactionDate]                           DATETIME        NULL,
    [ltfGlclubid]                               NVARCHAR (10)   NULL,
    [ltfEmployeeID]                             NVARCHAR (10)   NULL,
    [Quantity]                                  INT             NULL,
    [ItemAmount]                                DECIMAL (26, 6) NULL,
    [ItemDiscount]                              DECIMAL (26, 6) NULL,
    [OrderForEmployeeFlag]                      CHAR (1)        NULL,
    [dummy_modified_date_time]                  DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

