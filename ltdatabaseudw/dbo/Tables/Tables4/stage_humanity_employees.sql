CREATE TABLE [dbo].[stage_humanity_employees] (
    [stage_humanity_employees_id] BIGINT        NOT NULL,
    [employee_id]                 BIGINT        NULL,
    [employee_eid]                VARCHAR (255) NULL,
    [employee_name]               VARCHAR (255) NULL,
    [employee_email]              VARCHAR (255) NULL,
    [company_id]                  VARCHAR (255) NULL,
    [company_name]                VARCHAR (255) NULL,
    [deleted_flg]                 VARCHAR (255) NULL,
    [employee_status]             VARCHAR (255) NULL,
    [employee_role]               VARCHAR (255) NULL,
    [position_name]               VARCHAR (255) NULL,
    [location_name]               VARCHAR (255) NULL,
    [employee_to_see_wages]       VARCHAR (255) NULL,
    [last_active_date_utc]        VARCHAR (255) NULL,
    [user_timezone]               VARCHAR (255) NULL,
    [workday_position_id]         VARCHAR (255) NULL,
    [ltf_file_name]               VARCHAR (255) NULL,
    [file_arrive_date]            VARCHAR (255) NULL,
    [dummy_modified_date_time]    DATETIME      NULL,
    [dv_batch_id]                 BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

