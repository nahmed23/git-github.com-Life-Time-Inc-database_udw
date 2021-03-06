﻿CREATE TABLE [dbo].[wrk_boss_reservation_meeting_date] (
    [wrk_boss_reservation_meeting_date_id]      BIGINT       IDENTITY (1, 1) NOT NULL,
    [dim_boss_reservation_key]                  CHAR (32)    NULL,
    [dim_boss_reservation_meeting_dim_date_key] CHAR (32)    NULL,
    [end_dim_date_key]                          CHAR (8)     NULL,
    [instructor_type]                           CHAR (1)     NULL,
    [meeting_dim_date_key]                      CHAR (8)     NULL,
    [primary_dim_employee_key]                  CHAR (32)    NULL,
    [reservation_id]                            INT          NULL,
    [secondary_dim_employee_key]                CHAR (32)    NULL,
    [start_dim_date_key]                        CHAR (8)     NULL,
    [dv_load_date_time]                         DATETIME     NULL,
    [dv_load_end_date_time]                     DATETIME     NULL,
    [dv_batch_id]                               BIGINT       NOT NULL,
    [dv_inserted_date_time]                     DATETIME     NOT NULL,
    [dv_insert_user]                            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                      DATETIME     NULL,
    [dv_update_user]                            VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

