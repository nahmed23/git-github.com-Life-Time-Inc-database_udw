CREATE TABLE [dbo].[dv_job_diagnostics] (
    [ID]                     INT             IDENTITY (1, 1) NOT NULL,
    [job_name]               VARCHAR (256)   NULL,
    [average_runtime_7]      INT             NULL,
    [standard_deviation_7]   INT             NULL,
    [average_runtime_30]     INT             NULL,
    [standard_deviation_30]  INT             NULL,
    [average_runtime_90]     INT             NULL,
    [standard_deviation_90]  INT             NULL,
    [average_runtime_dow]    INT             NULL,
    [standard_deviation_dow] INT             NULL,
    [average_runtime_dom]    INT             NULL,
    [standard_deviation_dom] INT             NULL,
    [volatility_7]           DECIMAL (26, 6) NULL,
    [volatility_30]          DECIMAL (26, 6) NULL,
    [volatility_90]          DECIMAL (26, 6) NULL,
    [volatility_dow]         DECIMAL (26, 6) NULL,
    [volatility_dom]         DECIMAL (26, 6) NULL,
    [average_volatility]     DECIMAL (26, 6) NULL,
    [date_evaluated]         DATETIME        NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

