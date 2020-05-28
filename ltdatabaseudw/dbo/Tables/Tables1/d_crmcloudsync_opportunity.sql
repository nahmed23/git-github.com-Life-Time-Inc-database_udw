﻿CREATE TABLE [dbo].[d_crmcloudsync_opportunity] (
    [d_crmcloudsync_opportunity_id]                        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                              CHAR (32)       NOT NULL,
    [dim_crm_opportunity_key]                              VARCHAR (32)    NULL,
    [opportunity_id]                                       VARCHAR (36)    NULL,
    [actual_close_date]                                    DATETIME        NULL,
    [actual_close_dim_date_key]                            VARCHAR (8)     NULL,
    [actual_close_dim_time_key]                            INT             NULL,
    [created_by_dim_crm_system_user_key]                   VARCHAR (32)    NULL,
    [created_by_name]                                      NVARCHAR (200)  NULL,
    [created_dim_date_key]                                 VARCHAR (8)     NULL,
    [created_dim_time_key]                                 INT             NULL,
    [created_on]                                           DATETIME        NULL,
    [description]                                          NVARCHAR (4000) NULL,
    [dim_crm_ltf_club_key]                                 VARCHAR (32)    NULL,
    [dim_crm_owner_key]                                    VARCHAR (32)    NULL,
    [dim_crm_team_key]                                     VARCHAR (32)    NULL,
    [insert_user]                                          VARCHAR (100)   NULL,
    [inserted_date_time]                                   DATETIME        NULL,
    [inserted_dim_date_key]                                VARCHAR (8)     NULL,
    [inserted_dim_time_key]                                INT             NULL,
    [ltf_assigned_by_app]                                  INT             NULL,
    [ltf_assigned_by_app_name]                             NVARCHAR (255)  NULL,
    [ltf_assignment_request_date]                          DATETIME        NULL,
    [ltf_assignment_request_dim_date_key]                  VARCHAR (8)     NULL,
    [ltf_assignment_request_dim_time_key]                  INT             NULL,
    [ltf_assignment_request_id]                            NVARCHAR (100)  NULL,
    [ltf_channel]                                          INT             NULL,
    [ltf_channel_name]                                     NVARCHAR (255)  NULL,
    [ltf_club_id_name]                                     NVARCHAR (100)  NULL,
    [ltf_club_proximity]                                   INT             NULL,
    [ltf_club_proximity_name]                              NVARCHAR (255)  NULL,
    [ltf_commitment_level]                                 INT             NULL,
    [ltf_commitment_level_name]                            NVARCHAR (255)  NULL,
    [ltf_commitment_reason]                                NVARCHAR (4000) NULL,
    [ltf_exercise_history]                                 INT             NULL,
    [ltf_exercise_history_name]                            NVARCHAR (255)  NULL,
    [ltf_guest_pass_expiration_date]                       DATETIME        NULL,
    [ltf_guest_pass_expiration_dim_date_key]               VARCHAR (8)     NULL,
    [ltf_guest_pass_expiration_dim_time_key]               INT             NULL,
    [ltf_ims_join_link]                                    NVARCHAR (300)  NULL,
    [ltf_ims_join_send_date]                               DATETIME        NULL,
    [ltf_ims_join_send_dim_date_key]                       VARCHAR (8)     NULL,
    [ltf_ims_join_send_dim_time_key]                       INT             NULL,
    [ltf_injuries_or_limitations]                          BIT             NULL,
    [ltf_injuries_or_limitations_description]              NVARCHAR (4000) NULL,
    [ltf_injuries_or_limitations_flag]                     CHAR (1)        NULL,
    [ltf_injuries_or_limitations_name]                     NVARCHAR (255)  NULL,
    [ltf_is_ims_join]                                      BIT             NULL,
    [ltf_is_ims_join_flag]                                 CHAR (1)        NULL,
    [ltf_is_ims_join_name]                                 NVARCHAR (255)  NULL,
    [ltf_last_activity]                                    DATETIME        NULL,
    [ltf_last_activity_dim_date_key]                       VARCHAR (8)     NULL,
    [ltf_last_activity_dim_time_key]                       INT             NULL,
    [ltf_lead_source]                                      INT             NULL,
    [ltf_lead_source_name]                                 NVARCHAR (255)  NULL,
    [ltf_lead_type]                                        INT             NULL,
    [ltf_lead_type_name]                                   NVARCHAR (255)  NULL,
    [ltf_line_of_business]                                 INT             NULL,
    [ltf_line_of_business_name]                            NVARCHAR (255)  NULL,
    [ltf_managed_until]                                    DATETIME        NULL,
    [ltf_managed_until_dim_date_key]                       VARCHAR (8)     NULL,
    [ltf_managed_until_dim_time_key]                       INT             NULL,
    [ltf_measurable_goal]                                  INT             NULL,
    [ltf_measurable_goal_name]                             NVARCHAR (255)  NULL,
    [ltf_membership_level]                                 INT             NULL,
    [ltf_membership_level_name]                            NVARCHAR (255)  NULL,
    [ltf_membership_type]                                  INT             NULL,
    [ltf_membership_type_name]                             NVARCHAR (255)  NULL,
    [ltf_next_follow_up]                                   DATETIME        NULL,
    [ltf_next_follow_up_dim_date_key]                      VARCHAR (8)     NULL,
    [ltf_next_follow_up_dim_time_key]                      INT             NULL,
    [ltf_number_over_14_list]                              INT             NULL,
    [ltf_number_over_14_list_name]                         NVARCHAR (255)  NULL,
    [ltf_number_under_14_list]                             INT             NULL,
    [ltf_number_under_14_list_name]                        NVARCHAR (255)  NULL,
    [ltf_originating_guest_visit_fact_crm_guest_visit_key] VARCHAR (32)    NULL,
    [ltf_originating_guest_visit_name]                     NVARCHAR (200)  NULL,
    [ltf_park]                                             BIT             NULL,
    [ltf_park_comments]                                    NVARCHAR (400)  NULL,
    [ltf_park_flag]                                        CHAR (1)        NULL,
    [ltf_park_name]                                        NVARCHAR (255)  NULL,
    [ltf_park_reason]                                      INT             NULL,
    [ltf_park_reason_name]                                 NVARCHAR (255)  NULL,
    [ltf_park_until]                                       DATETIME        NULL,
    [ltf_park_until_dim_date_key]                          VARCHAR (8)     NULL,
    [ltf_park_until_dim_time_key]                          INT             NULL,
    [ltf_past_trainer_or_coach]                            BIT             NULL,
    [ltf_past_trainer_or_coach_flag]                       CHAR (1)        NULL,
    [ltf_past_trainer_or_coach_name]                       NVARCHAR (255)  NULL,
    [ltf_primary_objective]                                INT             NULL,
    [ltf_primary_objective_name]                           NVARCHAR (255)  NULL,
    [ltf_profile_notes]                                    NVARCHAR (4000) NULL,
    [ltf_programs_of_interest]                             INT             NULL,
    [ltf_programs_of_interest_name]                        NVARCHAR (255)  NULL,
    [ltf_promo_code]                                       NVARCHAR (100)  NULL,
    [ltf_promo_quoted]                                     NVARCHAR (100)  NULL,
    [ltf_ready_to_join]                                    BIT             NULL,
    [ltf_ready_to_join_flag]                               CHAR (1)        NULL,
    [ltf_ready_to_join_name]                               NVARCHAR (255)  NULL,
    [ltf_recommended_membership]                           INT             NULL,
    [ltf_recommended_membership_name]                      NVARCHAR (255)  NULL,
    [ltf_referring_contact_dim_crm_contact_key]            VARCHAR (32)    NULL,
    [ltf_referring_contact_id_name]                        NVARCHAR (160)  NULL,
    [ltf_referring_member_id]                              NVARCHAR (10)   NULL,
    [ltf_resistance]                                       INT             NULL,
    [ltf_resistance_name]                                  NVARCHAR (255)  NULL,
    [ltf_specific_goal]                                    INT             NULL,
    [ltf_specific_goal_name]                               NVARCHAR (255)  NULL,
    [ltf_time_goal]                                        DATETIME        NULL,
    [ltf_time_goal_dim_date_key]                           VARCHAR (8)     NULL,
    [ltf_time_goal_dim_time_key]                           INT             NULL,
    [ltf_todays_action]                                    INT             NULL,
    [ltf_todays_action_name]                               NVARCHAR (255)  NULL,
    [ltf_trainer_or_coach_preference]                      INT             NULL,
    [ltf_trainer_or_coach_preference_name]                 NVARCHAR (255)  NULL,
    [ltf_visitor_id]                                       NVARCHAR (100)  NULL,
    [ltf_want_to_do]                                       INT             NULL,
    [ltf_want_to_do_name]                                  NVARCHAR (255)  NULL,
    [ltf_web_team_id]                                      VARCHAR (36)    NULL,
    [ltf_web_team_id_name]                                 NVARCHAR (160)  NULL,
    [ltf_web_transfer_method]                              INT             NULL,
    [ltf_web_transfer_method_name]                         NVARCHAR (255)  NULL,
    [ltf_who_met_with]                                     NVARCHAR (100)  NULL,
    [ltf_why_want_to_do]                                   INT             NULL,
    [ltf_why_want_to_do_name]                              NVARCHAR (255)  NULL,
    [ltf_workout_preference]                               INT             NULL,
    [ltf_workout_preference_name]                          NVARCHAR (255)  NULL,
    [modified_by_dim_crm_system_user_key]                  VARCHAR (32)    NULL,
    [modified_by_name]                                     NVARCHAR (200)  NULL,
    [modified_dim_date_key]                                VARCHAR (8)     NULL,
    [modified_dim_time_key]                                INT             NULL,
    [modified_on]                                          DATETIME        NULL,
    [modified_on_behalf_by_dim_crm_system_user_key]        VARCHAR (32)    NULL,
    [name]                                                 NVARCHAR (300)  NULL,
    [originating_lead_dim_crm_lead_key]                    VARCHAR (32)    NULL,
    [originating_lead_id_name]                             NVARCHAR (160)  NULL,
    [overridden_created_dim_date_key]                      VARCHAR (8)     NULL,
    [overridden_created_dim_time_key]                      INT             NULL,
    [overridden_created_on]                                DATETIME        NULL,
    [owner_id]                                             VARCHAR (36)    NULL,
    [owner_id_name]                                        NVARCHAR (200)  NULL,
    [owner_id_type]                                        NVARCHAR (64)   NULL,
    [owning_business_unit]                                 VARCHAR (36)    NULL,
    [owning_user_dim_crm_system_user_key]                  VARCHAR (32)    NULL,
    [parent_account_dim_crm_account_key]                   VARCHAR (32)    NULL,
    [parent_account_id_name]                               NVARCHAR (160)  NULL,
    [parent_contact_dim_crm_contact_key]                   VARCHAR (32)    NULL,
    [parent_contact_id_name]                               NVARCHAR (160)  NULL,
    [state_code]                                           INT             NULL,
    [state_code_name]                                      NVARCHAR (255)  NULL,
    [status_code]                                          INT             NULL,
    [status_code_name]                                     NVARCHAR (255)  NULL,
    [total_amount]                                         DECIMAL (26, 6) NULL,
    [update_user]                                          VARCHAR (50)    NULL,
    [updated_date_time]                                    DATETIME        NULL,
    [updated_dim_date_key]                                 VARCHAR (8)     NULL,
    [updated_dim_time_key]                                 INT             NULL,
    [p_crmcloudsync_opportunity_id]                        BIGINT          NOT NULL,
    [deleted_flag]                                         INT             NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

