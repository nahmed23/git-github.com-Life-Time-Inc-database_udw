CREATE TABLE [sandbox_ebi].[job_monitoring_alerted] (
    [id]                  INT          IDENTITY (1, 1) NOT NULL,
    [job_name]            VARCHAR (50) NULL,
    [job_start_date_time] DATETIME     NULL,
    [job_status]          VARCHAR (15) NULL,
    [upper_bound]         INT          NULL,
    [lower_bound]         INT          NULL,
    [duration]            INT          NULL,
    [alerted_time]        DATETIME     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

