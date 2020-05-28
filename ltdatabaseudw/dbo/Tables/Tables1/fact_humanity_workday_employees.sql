CREATE TABLE [dbo].[fact_humanity_workday_employees] (
    [Cost_Center]                        VARCHAR (255)    NULL,
    [Hourly_Amount]                      VARCHAR (255)    NULL,
    [Job_Code]                           VARCHAR (255)    NULL,
    [Offering]                           VARCHAR (255)    NULL,
    [Region]                             VARCHAR (255)    NULL,
    [Position_ID]                        VARCHAR (255)    NULL,
    [Primary_Job]                        VARCHAR (255)    NULL,
    [Employee_ID]                        VARCHAR (255)    NULL,
    [created_user]                       VARCHAR (255)    NULL,
    [created_time]                       DATETIME         NULL,
    [Effective_date_begin]               DATE             NULL,
    [Effective_date_end]                 DATE             NULL,
    [File_arrive_date]                   VARCHAR (10)     NULL,
    [Employee_position_hashkey]          VARBINARY (8000) NULL,
    [Cost_Hour_Job_Offer_Region_hashkey] VARBINARY (8000) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

