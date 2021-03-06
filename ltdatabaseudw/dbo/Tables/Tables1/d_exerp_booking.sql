﻿CREATE TABLE [dbo].[d_exerp_booking] (
    [d_exerp_booking_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [booking_id]                   VARCHAR (4000) NULL,
    [age_text]                     VARCHAR (4000) NULL,
    [booking_name]                 VARCHAR (4000) NULL,
    [booking_state]                VARCHAR (4000) NULL,
    [cancel_dim_date_key]          CHAR (8)       NULL,
    [cancel_dim_time_key]          CHAR (8)       NULL,
    [cancel_reason]                VARCHAR (4000) NULL,
    [class_capacity]               INT            NULL,
    [color]                        VARCHAR (4000) NULL,
    [comment]                      VARCHAR (4000) NULL,
    [creation_dim_date_key]        CHAR (8)       NULL,
    [creation_dim_time_key]        CHAR (8)       NULL,
    [d_exerp_activity_bk_hash]     CHAR (32)      NULL,
    [d_exerp_center_bk_hash]       CHAR (32)      NULL,
    [description]                  VARCHAR (4000) NULL,
    [ets]                          BIGINT         NULL,
    [main_booking_id]              VARCHAR (4000) NULL,
    [main_d_exerp_booking_bk_hash] CHAR (32)      NULL,
    [max_capacity_override]        INT            NULL,
    [maximum_age]                  INT            NULL,
    [maximum_age_unit]             VARCHAR (4000) NULL,
    [minimum_age]                  INT            NULL,
    [minimum_age_unit]             VARCHAR (4000) NULL,
    [single_cancellation_flag]     CHAR (1)       NULL,
    [start_dim_date_key]           CHAR (8)       NULL,
    [start_dim_time_key]           CHAR (8)       NULL,
    [stop_dim_date_key]            CHAR (8)       NULL,
    [stop_dim_time_key]            CHAR (8)       NULL,
    [strict_age_limit]             INT            NULL,
    [waiting_list_capacity]        INT            NULL,
    [p_exerp_booking_id]           BIGINT         NOT NULL,
    [deleted_flag]                 INT            NULL,
    [dv_load_date_time]            DATETIME       NULL,
    [dv_load_end_date_time]        DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

