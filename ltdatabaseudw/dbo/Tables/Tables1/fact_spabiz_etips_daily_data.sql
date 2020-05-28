CREATE TABLE [dbo].[fact_spabiz_etips_daily_data] (
    [fact_spabiz_etips_daily_data_key] BIGINT          NOT NULL,
    [spabiz_etips_daily_data_key]      VARCHAR (255)   NOT NULL,
    [employee_id]                      VARCHAR (100)   NULL,
    [first_last_name]                  VARCHAR (100)   NULL,
    [tip_amount]                       DECIMAL (26, 2) NULL,
    [created_date]                     DATETIME        NULL,
    [location_codevalue]               VARCHAR (100)   NULL,
    [response_status_code]             VARCHAR (100)   NULL,
    [response_reference_code]          VARCHAR (100)   NULL,
    [error_description]                VARCHAR (1000)  NULL,
    [inserted_date_time]               DATETIME        NULL,
    [inserted_user]                    VARCHAR (100)   NULL,
    [adp_response_time]                VARCHAR (255)   NULL,
    [transaction_reference_id]         BIGINT          NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

