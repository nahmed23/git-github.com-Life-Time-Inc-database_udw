CREATE TABLE [dbo].[stage_spabiz_etips_daily_data_detail] (
    [Employee_id]        VARCHAR (255) NULL,
    [Employee_Name]      VARCHAR (255) NULL,
    [tip_amount]         VARCHAR (255) NULL,
    [created_date_time]  VARCHAR (255) NULL,
    [customer_name]      VARCHAR (255) NULL,
    [store_number]       VARCHAR (255) NULL,
    [location_codevalue] VARCHAR (255) NULL,
    [workday_region]     VARCHAR (255) NULL,
    [club_name]          VARCHAR (255) NULL,
    [formal_club_name]   VARCHAR (255) NULL,
    [Store_name]         VARCHAR (255) NULL,
    [tender_type]        VARCHAR (255) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

