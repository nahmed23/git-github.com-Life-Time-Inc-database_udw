CREATE TABLE [dbo].[stage_spabiz_etips_daily_data_optin] (
    [stage_spabiz_etips_daily_data_key] BIGINT          NOT NULL,
    [spabiz_etips_daily_data_key]       VARCHAR (255)   NOT NULL,
    [employee_id]                       VARCHAR (100)   NULL,
    [first_last_name]                   VARCHAR (100)   NULL,
    [tip_amount]                        DECIMAL (26, 2) NULL,
    [created_date]                      DATETIME        NULL,
    [store_number]                      INT             NULL,
    [location_codevalue]                VARCHAR (100)   NULL,
    [workday_region]                    VARCHAR (10)    NULL,
    [tender_type]                       VARCHAR (100)   NULL,
    [customer_name]                     VARCHAR (255)   NULL,
    [club_name]                         VARCHAR (255)   NULL,
    [formal_club_name]                  VARCHAR (255)   NULL,
    [Store_name]                        VARCHAR (255)   NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

