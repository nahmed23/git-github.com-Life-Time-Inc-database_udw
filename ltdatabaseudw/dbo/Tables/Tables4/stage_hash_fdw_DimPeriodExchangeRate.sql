CREATE TABLE [dbo].[stage_hash_fdw_DimPeriodExchangeRate] (
    [stage_hash_fdw_DimPeriodExchangeRate_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [DimPeriodExchangeRateKey]                INT             NULL,
    [DimAccountingPeriodKey]                  INT             NULL,
    [FromCurrencyCode]                        VARCHAR (10)    NULL,
    [ToCurrencyCode]                          VARCHAR (10)    NULL,
    [BudgetRate]                              DECIMAL (14, 4) NULL,
    [PlanRate]                                DECIMAL (14, 4) NULL,
    [InsertedDateTime]                        DATETIME        NULL,
    [InsertUser]                              VARCHAR (50)    NULL,
    [UpdatedDateTime]                         DATETIME        NULL,
    [UpdatedUser]                             VARCHAR (50)    NULL,
    [BatchID]                                 BIGINT          NULL,
    [dv_load_date_time]                       DATETIME        NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL,
    [dv_batch_id]                             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

