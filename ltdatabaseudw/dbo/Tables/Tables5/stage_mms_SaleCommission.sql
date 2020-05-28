CREATE TABLE [dbo].[stage_mms_SaleCommission] (
    [stage_mms_SaleCommission_id] BIGINT   NOT NULL,
    [SaleCommissionID]            INT      NULL,
    [TranItemID]                  INT      NULL,
    [EmployeeID]                  INT      NULL,
    [InsertedDateTime]            DATETIME NULL,
    [UpdatedDateTime]             DATETIME NULL,
    [dv_batch_id]                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

