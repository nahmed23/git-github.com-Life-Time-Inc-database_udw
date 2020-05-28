CREATE TABLE [dbo].[fact_humanity_employees] (
    [employee_id]           BIGINT        NOT NULL,
    [employee_eid]          VARCHAR (255) NULL,
    [employee_name]         VARCHAR (255) NULL,
    [employee_email]        VARCHAR (255) NULL,
    [company_id]            VARCHAR (255) NULL,
    [company_name]          VARCHAR (255) NULL,
    [deleted_flg]           VARCHAR (255) NULL,
    [employee_status]       VARCHAR (255) NULL,
    [employee_role]         VARCHAR (255) NULL,
    [position_name]         VARCHAR (255) NULL,
    [location_name]         VARCHAR (255) NULL,
    [employee_to_see_wages] VARCHAR (255) NULL,
    [last_active_date_utc]  VARCHAR (255) NULL,
    [user_timezone]         VARCHAR (255) NULL,
    [Inserted_date_time]    DATETIME      NULL,
    [Inserted_user]         VARCHAR (255) NULL,
    [file_arrive_date]      VARCHAR (255) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

