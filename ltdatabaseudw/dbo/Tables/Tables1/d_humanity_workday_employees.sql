﻿CREATE TABLE [dbo].[d_humanity_workday_employees] (
    [d_humanity_workday_employees_id]    BIGINT           IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)        NOT NULL,
    [d_humanity_workday_employees_key]   CHAR (32)        NULL,
    [employee_id]                        BIGINT           NULL,
    [wd_file_name]                       VARCHAR (255)    NULL,
    [hire_date]                          VARCHAR (255)    NULL,
    [term_date]                          VARCHAR (255)    NULL,
    [employee_status]                    VARCHAR (255)    NULL,
    [effective_date_for_position]        VARCHAR (255)    NULL,
    [sup_org_ref_id]                     VARCHAR (255)    NULL,
    [supervisory_organization]           VARCHAR (255)    NULL,
    [job_profile]                        VARCHAR (255)    NULL,
    [manager]                            VARCHAR (255)    NULL,
    [location_id]                        VARCHAR (255)    NULL,
    [time_in_job_profile]                VARCHAR (255)    NULL,
    [company_id]                         VARCHAR (255)    NULL,
    [anticipated_weekly_work_hours]      VARCHAR (255)    NULL,
    [pay_type]                           VARCHAR (255)    NULL,
    [class_rate]                         VARCHAR (255)    NULL,
    [commission_plans]                   VARCHAR (255)    NULL,
    [position_id]                        VARCHAR (255)    NULL,
    [hourly_amount]                      VARCHAR (255)    NULL,
    [job_code]                           VARCHAR (255)    NULL,
    [offering]                           VARCHAR (255)    NULL,
    [region]                             VARCHAR (255)    NULL,
    [primary_job]                        VARCHAR (255)    NULL,
    [cost_center]                        VARCHAR (255)    NULL,
    [cost_hour_job_offer_region_hashkey] VARBINARY (8000) NULL,
    [effective_date_begin]               DATE             NULL,
    [effective_date_end]                 DATE             NULL,
    [employee_position_hashkey]          VARBINARY (8000) NULL,
    [file_arrive_date]                   DATE             NULL,
    [worker]                             VARCHAR (255)    NULL,
    [p_humanity_workday_employees_id]    BIGINT           NOT NULL,
    [deleted_flag]                       INT              NULL,
    [dv_load_date_time]                  DATETIME         NULL,
    [dv_load_end_date_time]              DATETIME         NULL,
    [dv_batch_id]                        BIGINT           NOT NULL,
    [dv_inserted_date_time]              DATETIME         NOT NULL,
    [dv_insert_user]                     VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]               DATETIME         NULL,
    [dv_update_user]                     VARCHAR (50)     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

