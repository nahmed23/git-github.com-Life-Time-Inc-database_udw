CREATE TABLE [dbo].[stage_spabiz_etips_m_locations] (
    [tender_type]                 VARCHAR (100) NULL,
    [spabiz_etips_daily_data_key] VARCHAR (255) NULL,
    [employee_id]                 VARCHAR (100) NULL,
    [first_last_name]             VARCHAR (100) NULL,
    [tip_amount]                  DECIMAL (18)  NULL,
    [created_date]                VARCHAR (100) NULL,
    [store_number]                INT           NULL,
    [location_codevalue]          VARCHAR (100) NULL,
    [workday_region]              VARCHAR (10)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

