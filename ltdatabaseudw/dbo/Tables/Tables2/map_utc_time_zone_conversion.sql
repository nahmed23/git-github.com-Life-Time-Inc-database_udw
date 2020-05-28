CREATE TABLE [dbo].[map_utc_time_zone_conversion] (
    [map_utc_time_zone_conversion_key] INT          IDENTITY (1, 1) NOT NULL,
    [val_time_zone_id]                 INT          NULL,
    [description]                      VARCHAR (50) NULL,
    [daylight_saving_flag]             TINYINT      NULL,
    [utc_start_date_time]              DATETIME     NULL,
    [utc_end_date_time]                DATETIME     NULL,
    [offset]                           INT          NULL
)
WITH (CLUSTERED INDEX([map_utc_time_zone_conversion_key]), DISTRIBUTION = REPLICATE);

