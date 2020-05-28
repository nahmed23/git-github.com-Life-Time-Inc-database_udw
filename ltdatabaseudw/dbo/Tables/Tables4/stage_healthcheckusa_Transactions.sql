CREATE TABLE [dbo].[stage_healthcheckusa_Transactions] (
    [stage_healthcheckusa_Transactions_id] BIGINT          NOT NULL,
    [OrderNumber]                          INT             NULL,
    [SKU]                                  INT             NULL,
    [TransactionType]                      VARCHAR (100)   NULL,
    [TransactionDate]                      DATETIME        NULL,
    [ltfGlclubid]                          NVARCHAR (10)   NULL,
    [ltfEmployeeID]                        NVARCHAR (10)   NULL,
    [Quantity]                             INT             NULL,
    [ItemAmount]                           DECIMAL (26, 6) NULL,
    [ItemDiscount]                         DECIMAL (26, 6) NULL,
    [OrderForEmployeeFlag]                 CHAR (1)        NULL,
    [dummy_modified_date_time]             DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

