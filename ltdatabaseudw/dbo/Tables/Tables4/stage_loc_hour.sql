CREATE TABLE [dbo].[stage_loc_hour] (
    [stage_loc_hour_id]      BIGINT        NOT NULL,
    [hour_id]                BIGINT        NULL,
    [location_id]            BIGINT        NULL,
    [val_hour_type_id]       BIGINT        NULL,
    [day_of_week]            CHAR (3)      NULL,
    [start_time]             TIME (7)      NULL,
    [end_time]               TIME (7)      NULL,
    [hour_24]                BIT           NULL,
    [sunrise]                BIT           NULL,
    [sunset]                 BIT           NULL,
    [closed]                 BIT           NULL,
    [by_appointment_only]    BIT           NULL,
    [created_date_time]      DATETIME      NULL,
    [created_by]             VARCHAR (100) NULL,
    [deleted_date_time]      DATETIME      NULL,
    [deleted_by]             VARCHAR (100) NULL,
    [last_updated_date_time] DATETIME      NULL,
    [last_updated_by]        VARCHAR (100) NULL,
    [udw_dim_location_key]   VARCHAR (32)  NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

