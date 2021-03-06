﻿CREATE TABLE [dbo].[d_crmcloudsync_appointment] (
    [d_crmcloudsync_appointment_id]                 BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)      NOT NULL,
    [fact_crm_appointment_key]                      VARCHAR (32)   NULL,
    [activity_id]                                   VARCHAR (36)   NULL,
    [activity_type_code]                            NVARCHAR (64)  NULL,
    [activity_type_code_name]                       NVARCHAR (255) NULL,
    [actual_duration_minutes]                       INT            NULL,
    [actual_end]                                    DATETIME       NULL,
    [actual_end_dim_date_key]                       VARCHAR (8)    NULL,
    [actual_end_dim_time_key]                       INT            NULL,
    [actual_start]                                  DATETIME       NULL,
    [actual_start_dim_date_key]                     VARCHAR (8)    NULL,
    [actual_start_dim_time_key]                     INT            NULL,
    [category]                                      NVARCHAR (250) NULL,
    [created_by_dim_crm_system_user_key]            VARCHAR (32)   NULL,
    [created_by_name]                               NVARCHAR (200) NULL,
    [created_dim_date_key]                          VARCHAR (8)    NULL,
    [created_dim_time_key]                          INT            NULL,
    [created_on]                                    DATETIME       NULL,
    [created_on_behalf_by_dim_crm_system_user_key]  VARCHAR (32)   NULL,
    [created_on_behalf_by_name]                     NVARCHAR (200) NULL,
    [description]                                   VARCHAR (8000) NULL,
    [dim_crm_ltf_club_key]                          VARCHAR (32)   NULL,
    [dim_crm_owner_key]                             VARCHAR (32)   NULL,
    [dim_crm_regarding_object_key]                  VARCHAR (32)   NULL,
    [insert_user]                                   VARCHAR (100)  NULL,
    [inserted_date_time]                            DATETIME       NULL,
    [inserted_dim_date_key]                         VARCHAR (8)    NULL,
    [inserted_dim_time_key]                         INT            NULL,
    [instance_type_code]                            INT            NULL,
    [instance_type_code_name]                       NVARCHAR (255) NULL,
    [ltf_appointment_type]                          INT            NULL,
    [ltf_appointment_type_name]                     NVARCHAR (255) NULL,
    [ltf_check_in_flag]                             INT            NULL,
    [ltf_check_in_flag_name]                        NVARCHAR (255) NULL,
    [ltf_club_id_name]                              NVARCHAR (100) NULL,
    [ltf_program]                                   INT            NULL,
    [ltf_program_name]                              NVARCHAR (255) NULL,
    [ltf_qr_code]                                   VARCHAR (8000) NULL,
    [ltf_udw_id]                                    NVARCHAR (255) NULL,
    [ltf_web_booking_source]                        INT            NULL,
    [ltf_web_booking_source_name]                   NVARCHAR (255) NULL,
    [modified_by_dim_crm_system_user_key]           VARCHAR (32)   NULL,
    [modified_by_name]                              NVARCHAR (200) NULL,
    [modified_dim_date_key]                         VARCHAR (8)    NULL,
    [modified_dim_time_key]                         INT            NULL,
    [modified_on]                                   DATETIME       NULL,
    [modified_on_behalf_by_dim_crm_system_user_key] VARCHAR (32)   NULL,
    [modified_on_behalf_by_name]                    NVARCHAR (200) NULL,
    [owner_id_name]                                 NVARCHAR (200) NULL,
    [owner_id_type]                                 NVARCHAR (64)  NULL,
    [owning_business_unit]                          VARCHAR (36)   NULL,
    [owning_team]                                   VARCHAR (36)   NULL,
    [owning_user_dim_crm_system_user_key]           VARCHAR (32)   NULL,
    [priority_code]                                 INT            NULL,
    [priority_code_name]                            VARCHAR (255)  NULL,
    [regarding_object_id_name]                      VARCHAR (8000) NULL,
    [regarding_object_type_code]                    NVARCHAR (64)  NULL,
    [scheduled_duration_minutes]                    INT            NULL,
    [scheduled_end]                                 DATETIME       NULL,
    [scheduled_end_dim_date_key]                    VARCHAR (8)    NULL,
    [scheduled_end_dim_time_key]                    INT            NULL,
    [scheduled_start]                               DATETIME       NULL,
    [scheduled_start_dim_date_key]                  VARCHAR (8)    NULL,
    [scheduled_start_dim_time_key]                  INT            NULL,
    [state_code]                                    INT            NULL,
    [state_code_name]                               NVARCHAR (255) NULL,
    [status_code]                                   INT            NULL,
    [status_code_name]                              NVARCHAR (255) NULL,
    [subject]                                       NVARCHAR (200) NULL,
    [time_zone_rule_version_number]                 INT            NULL,
    [update_user]                                   VARCHAR (50)   NULL,
    [updated_date_time]                             DATETIME       NULL,
    [updated_dim_date_key]                          VARCHAR (8)    NULL,
    [updated_dim_time_key]                          INT            NULL,
    [utc_conversion_time_zone_code]                 INT            NULL,
    [p_crmcloudsync_appointment_id]                 BIGINT         NOT NULL,
    [deleted_flag]                                  INT            NULL,
    [dv_load_date_time]                             DATETIME       NULL,
    [dv_load_end_date_time]                         DATETIME       NULL,
    [dv_batch_id]                                   BIGINT         NOT NULL,
    [dv_inserted_date_time]                         DATETIME       NOT NULL,
    [dv_insert_user]                                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                          DATETIME       NULL,
    [dv_update_user]                                VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

