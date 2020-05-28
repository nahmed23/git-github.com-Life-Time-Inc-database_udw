﻿CREATE TABLE [dbo].[fact_exerp_clipcard_usage] (
    [fact_exerp_clipcard_usage_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [access_privilege_usage_state]            VARCHAR (4000) NULL,
    [cancelled_flag]                          CHAR (1)       NULL,
    [clipcard_blocked_flag]                   CHAR (1)       NULL,
    [clipcard_cancelled_flag]                 CHAR (1)       NULL,
    [clipcard_usage_entered_dim_employee_key] VARCHAR (32)   NULL,
    [clipcard_usage_id]                       INT            NULL,
    [clipcard_usage_state]                    VARCHAR (4000) NULL,
    [clipcard_usage_type]                     VARCHAR (4000) NULL,
    [clips]                                   INT            NULL,
    [clips_initial]                           INT            NULL,
    [commission_units]                        INT            NULL,
    [delivered_dim_club_key]                  VARCHAR (32)   NULL,
    [delivered_dim_employee_key]              VARCHAR (32)   NULL,
    [dim_exerp_booking_key]                   VARCHAR (32)   NULL,
    [dim_exerp_clipcard_key]                  VARCHAR (32)   NULL,
    [dim_exerp_product_key]                   VARCHAR (32)   NULL,
    [dim_mms_member_key]                      VARCHAR (32)   NULL,
    [fact_exerp_clipcard_usage_key]           VARCHAR (32)   NULL,
    [participation_complete_flag]             CHAR (1)       NULL,
    [sale_entered_dim_employee_key]           VARCHAR (32)   NULL,
    [sale_entry_dim_date_key]                 VARCHAR (8)    NULL,
    [sale_entry_dim_time_key]                 INT            NULL,
    [sale_source_type]                        VARCHAR (4000) NULL,
    [usage_dim_date_key]                      VARCHAR (8)    NULL,
    [usage_dim_time_key]                      INT            NULL,
    [dv_load_date_time]                       DATETIME       NULL,
    [dv_load_end_date_time]                   DATETIME       NULL,
    [dv_batch_id]                             BIGINT         NOT NULL,
    [dv_inserted_date_time]                   DATETIME       NOT NULL,
    [dv_insert_user]                          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                    DATETIME       NULL,
    [dv_update_user]                          VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_exerp_clipcard_usage_key]));

