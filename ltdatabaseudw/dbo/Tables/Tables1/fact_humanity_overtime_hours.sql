﻿CREATE TABLE [dbo].[fact_humanity_overtime_hours] (
    [fact_humanity_overtime_hours_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [company_id]                       VARCHAR (255) NULL,
    [date_formatted]                   VARCHAR (255) NULL,
    [deleted_flag]                     VARCHAR (255) NULL,
    [employee_id]                      VARCHAR (255) NULL,
    [employee_name]                    VARCHAR (255) NULL,
    [end_time]                         VARCHAR (255) NULL,
    [fact_humanity_overtime_hours_key] CHAR (32)     NULL,
    [file_arrive_date]                 DATE          NULL,
    [hours_d_overtime]                 VARCHAR (255) NULL,
    [hours_location_id]                VARCHAR (255) NULL,
    [hours_overtime]                   VARCHAR (255) NULL,
    [hours_position_id]                VARCHAR (255) NULL,
    [hours_regular]                    VARCHAR (255) NULL,
    [ot_date_formatted_dim_date_key]   VARCHAR (255) NULL,
    [ot_end_time_dim_time_key]         CHAR (8)      NULL,
    [ot_start_time_dim_time_key]       CHAR (8)      NULL,
    [start_time]                       VARCHAR (255) NULL,
    [userid]                           BIGINT        NULL,
    [dv_load_date_time]                DATETIME      NULL,
    [dv_load_end_date_time]            DATETIME      NULL,
    [dv_batch_id]                      BIGINT        NOT NULL,
    [dv_inserted_date_time]            DATETIME      NOT NULL,
    [dv_insert_user]                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]             DATETIME      NULL,
    [dv_update_user]                   VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_humanity_overtime_hours_key]));
