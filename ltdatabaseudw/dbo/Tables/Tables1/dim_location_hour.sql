﻿CREATE TABLE [dbo].[dim_location_hour] (
    [dim_location_hour_id]             BIGINT         IDENTITY (1, 1) NOT NULL,
    [by_appointment_only_flag]         CHAR (1)       NULL,
    [closed_flag]                      CHAR (1)       NULL,
    [created_by]                       VARCHAR (100)  NULL,
    [created_date_time]                DATETIME       NULL,
    [day_of_week]                      CHAR (3)       NULL,
    [deleted_by]                       VARCHAR (100)  NULL,
    [deleted_date_time]                DATETIME       NULL,
    [deleted_flag]                     INT            NULL,
    [dim_location_hour_key]            VARCHAR (32)   NULL,
    [dim_location_key]                 VARCHAR (32)   NULL,
    [end_dim_time_key]                 INT            NULL,
    [end_time]                         TIME (7)       NULL,
    [hour_24_flag]                     CHAR (1)       NULL,
    [hour_id]                          BIGINT         NULL,
    [last_updated_by]                  VARCHAR (100)  NULL,
    [last_updated_date_time]           DATETIME       NULL,
    [start_dim_time_key]               INT            NULL,
    [start_time]                       TIME (7)       NULL,
    [sunrise_flag]                     CHAR (1)       NULL,
    [sunset_flag]                      CHAR (1)       NULL,
    [updated_dim_date_key]             VARCHAR (8)    NULL,
    [updated_dim_time_key]             INT            NULL,
    [val_hour_type_display_name]       VARCHAR (4000) NULL,
    [val_hour_type_group_display_name] VARCHAR (4000) NULL,
    [val_hour_type_group_name]         VARCHAR (100)  NULL,
    [val_hour_type_name]               VARCHAR (100)  NULL,
    [dv_load_date_time]                DATETIME       NULL,
    [dv_load_end_date_time]            DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL,
    [dv_inserted_date_time]            DATETIME       NOT NULL,
    [dv_insert_user]                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]             DATETIME       NULL,
    [dv_update_user]                   VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_location_hour_key]));
