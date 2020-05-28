CREATE TABLE [dbo].[stage_fdw_DimPeriodExchangeRate] (
    [stage_fdw_DimPeriodExchangeRate_id] BIGINT          NOT NULL,
    [DimPeriodExchangeRateKey]           INT             NULL,
    [DimAccountingPeriodKey]             INT             NULL,
    [FromCurrencyCode]                   VARCHAR (10)    NULL,
    [ToCurrencyCode]                     VARCHAR (10)    NULL,
    [BudgetRate]                         DECIMAL (14, 4) NULL,
    [PlanRate]                           DECIMAL (14, 4) NULL,
    [InsertedDateTime]                   DATETIME        NULL,
    [InsertUser]                         VARCHAR (50)    NULL,
    [UpdatedDateTime]                    DATETIME        NULL,
    [UpdatedUser]                        VARCHAR (50)    NULL,
    [BatchID]                            BIGINT          NULL,
    [dv_batch_id]                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

