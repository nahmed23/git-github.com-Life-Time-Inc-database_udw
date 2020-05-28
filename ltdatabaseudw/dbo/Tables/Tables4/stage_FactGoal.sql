CREATE TABLE [dbo].[stage_FactGoal] (
    [clubid]                                        INT             NULL,
    [DimReportingHierarchyKey]                      INT             NULL,
    [GoalDollarAmount]                              DECIMAL (12, 2) NULL,
    [GoalEffectiveDimDateKey]                       CHAR (8)        NULL,
    [LocalCurrencyMonthlyAverageDimExchangeRateKey] INT             NULL,
    [OriginalCurrencyCode]                          CHAR (3)        NULL,
    [USDMonthlyAverageDimExchangeRateKey]           INT             NULL,
    [dv_load_date_time]                             DATETIME        NULL,
    [dv_load_end_date_time]                         DATETIME        NULL,
    [dv_batch_id]                                   BIGINT          NOT NULL,
    [dv_inserted_date_time]                         DATETIME        NOT NULL,
    [dv_insert_user]                                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                          DATETIME        NULL,
    [dv_update_user]                                VARCHAR (50)    NULL,
    [dim_reporting_hierarchy_key]                   VARCHAR (32)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

