CREATE TABLE [dbo].[stage_spabiz_etips_daily_data] (
    [stage_spabiz_etips_daily_data_key] BIGINT          NOT NULL,
    [spabiz_etips_daily_data_key]       VARCHAR (255)   NOT NULL,
    [employee_id]                       VARCHAR (100)   NULL,
    [first_last_name]                   VARCHAR (100)   NULL,
    [tip_amount]                        DECIMAL (26, 2) NULL,
    [created_date]                      DATETIME        NULL,
    [store_number]                      INT             NULL,
    [location_codevalue]                VARCHAR (100)   NULL,
    [workday_region]                    VARCHAR (10)    NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

