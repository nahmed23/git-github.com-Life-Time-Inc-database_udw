CREATE TABLE [dbo].[exerp_managed_club_department] (
    [exerp_managed_club_department_id] INT         NULL,
    [club_id]                          INT         NULL,
    [department_id]                    INT         NULL,
    [effective_from_date_time]         DATETIME    NULL,
    [effective_thru_date_time]         DATETIME    NULL,
    [inserted_date_time]               DATETIME    NULL,
    [updated_date_time]                DATETIME    NULL,
    [migration_dim_date_key]           VARCHAR (8) NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

