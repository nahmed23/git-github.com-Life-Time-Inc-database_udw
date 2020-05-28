﻿CREATE TABLE [dbo].[d_exerp_activity] (
    [d_exerp_activity_id]              BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)      NOT NULL,
    [activity_id]                      INT            NULL,
    [access_group_id]                  INT            NULL,
    [activity_name]                    VARCHAR (4000) NULL,
    [activity_state]                   VARCHAR (4000) NULL,
    [activity_type]                    VARCHAR (4000) NULL,
    [age_group_id]                     INT            NULL,
    [color]                            VARCHAR (4000) NULL,
    [course_schedule_type]             VARCHAR (4000) NULL,
    [d_exerp_activity_group_bk_hash]   CHAR (32)      NULL,
    [d_exerp_age_group_bk_hash]        VARCHAR (32)   NULL,
    [description]                      VARCHAR (4000) NULL,
    [dim_boss_product_key]             CHAR (32)      NULL,
    [dim_exerp_time_configuration_key] VARCHAR (32)   NULL,
    [external_id]                      VARCHAR (4000) NULL,
    [max_participants]                 INT            NULL,
    [max_waiting_list_participants]    INT            NULL,
    [time_configuration_id]            INT            NULL,
    [p_exerp_activity_id]              BIGINT         NOT NULL,
    [deleted_flag]                     INT            NULL,
    [dv_load_date_time]                DATETIME       NULL,
    [dv_load_end_date_time]            DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL,
    [dv_inserted_date_time]            DATETIME       NOT NULL,
    [dv_insert_user]                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]             DATETIME       NULL,
    [dv_update_user]                   VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));
