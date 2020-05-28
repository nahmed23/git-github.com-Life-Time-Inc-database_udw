CREATE TABLE [dbo].[fact_tips_terminated_employees] (
    [fact_tips_terminated_employees_key] BIGINT          IDENTITY (1, 1) NOT NULL,
    [Employee_ID]                        VARCHAR (30)    NULL,
    [Employee_Name]                      VARCHAR (100)   NULL,
    [store_number]                       VARCHAR (30)    NULL,
    [store_name]                         VARCHAR (150)   NULL,
    [Tip_Amount]                         DECIMAL (26, 2) NULL,
    [created_date_time]                  DATETIME        NULL,
    [Workday_region]                     VARCHAR (10)    NULL,
    [Termination_Date]                   DATETIME        NULL,
    [Week_Number]                        INT             NULL,
    [Year]                               VARCHAR (4)     NULL,
    [active_status]                      VARCHAR (4)     NULL,
    [hire_date]                          DATETIME        NULL,
    [report_description]                 VARCHAR (40)    NULL,
    [inserted_date_time]                 DATETIME        NULL,
    [inserted_user]                      VARCHAR (100)   NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

