﻿CREATE TABLE [dbo].[fact_trainerize_workout_history] (
    [fact_trainerize_workout_history_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [active_flag]                         CHAR (1)        NULL,
    [activity_type]                       VARCHAR (8000)  NULL,
    [average_heart_rate]                  DECIMAL (28, 8) NULL,
    [average_miles_per_hour]              DECIMAL (28, 8) NULL,
    [average_watts]                       DECIMAL (28, 8) NULL,
    [comments]                            VARCHAR (8000)  NULL,
    [completed_flag]                      CHAR (1)        NULL,
    [created_dim_date_key]                VARCHAR (8)     NULL,
    [custom_flag]                         CHAR (1)        NULL,
    [dim_mms_member_key]                  VARCHAR (32)    NULL,
    [dim_trainerize_workout_key]          VARCHAR (32)    NULL,
    [distance_in_miles]                   DECIMAL (26, 6) NULL,
    [ended_dim_date_key]                  VARCHAR (8)     NULL,
    [ended_dim_time_key]                  INT             NULL,
    [fact_trainerize_workout_history_key] VARCHAR (32)    NULL,
    [fat_calories]                        INT             NULL,
    [heart_rate_zone_five_seconds]        INT             NULL,
    [heart_rate_zone_four_seconds]        INT             NULL,
    [heart_rate_zone_one_seconds]         INT             NULL,
    [heart_rate_zone_three_seconds]       INT             NULL,
    [heart_rate_zone_two_seconds]         INT             NULL,
    [key_value]                           VARCHAR (8000)  NULL,
    [rating]                              INT             NULL,
    [scheduled_flag]                      CHAR (1)        NULL,
    [source_name]                         VARCHAR (8000)  NULL,
    [source_workout_id]                   VARCHAR (8000)  NULL,
    [started_dim_date_key]                VARCHAR (8)     NULL,
    [started_dim_time_key]                INT             NULL,
    [started_flag]                        CHAR (1)        NULL,
    [total_calories]                      INT             NULL,
    [tracked_flag]                        CHAR (1)        NULL,
    [workout_description]                 VARCHAR (8000)  NULL,
    [workout_history_id]                  INT             NULL,
    [workout_type]                        VARCHAR (8000)  NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_trainerize_workout_history_key]));

