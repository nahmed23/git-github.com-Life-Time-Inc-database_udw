CREATE TABLE [dbo].[dv_job_diagnostics_labeled] (
    [ID]                         INT           IDENTITY (1, 1) NOT NULL,
    [job_name]                   VARCHAR (256) NULL,
    [ninety_day_runtime_trend]   VARCHAR (18)  NULL,
    [weekday_runtime_trend]      VARCHAR (18)  NULL,
    [day_of_month_runtime_trend] VARCHAR (18)  NULL,
    [volatility_rating]          VARCHAR (18)  NULL,
    [date_evaluated]             DATETIME      NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

