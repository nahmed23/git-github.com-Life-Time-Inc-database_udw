﻿CREATE TABLE [dbo].[d_crmcloudsync_contact] (
    [d_crmcloudsync_contact_id]                         BIGINT           IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)        NOT NULL,
    [dim_crm_contact_key]                               VARCHAR (32)     NULL,
    [contact_id]                                        VARCHAR (36)     NULL,
    [address_1_city]                                    NVARCHAR (80)    NULL,
    [address_1_composite]                               NVARCHAR (4000)  NULL,
    [address_1_country]                                 NVARCHAR (80)    NULL,
    [address_1_line_1]                                  NVARCHAR (250)   NULL,
    [address_1_line_2]                                  NVARCHAR (250)   NULL,
    [address_1_line_3]                                  NVARCHAR (250)   NULL,
    [address_1_postal_code]                             NVARCHAR (20)    NULL,
    [address_1_state_or_province]                       NVARCHAR (50)    NULL,
    [address_1_telephone_1]                             NVARCHAR (50)    NULL,
    [birth_date]                                        DATETIME         NULL,
    [birth_dim_date_key]                                VARCHAR (8)      NULL,
    [created_by_dim_crm_system_user_key]                VARCHAR (32)     NULL,
    [created_dim_date_key]                              VARCHAR (8)      NULL,
    [created_dim_time_key]                              INT              NULL,
    [created_on]                                        DATETIME         NULL,
    [created_on_behalf_by_dim_crm_system_user_key]      CHAR (32)        NULL,
    [dim_crm_address_1_address_key]                     VARCHAR (32)     NULL,
    [dim_crm_ltf_club_key]                              VARCHAR (32)     NULL,
    [dim_crm_ltf_employer_key]                          VARCHAR (32)     NULL,
    [dim_crm_ltf_ltf_party_key]                         VARCHAR (32)     NULL,
    [dim_crm_owner_key]                                 VARCHAR (32)     NULL,
    [dim_crm_team_key]                                  VARCHAR (32)     NULL,
    [dim_mms_member_key]                                VARCHAR (32)     NULL,
    [do_not_email]                                      BIT              NULL,
    [do_not_email_flag]                                 CHAR (1)         NULL,
    [do_not_email_name]                                 NVARCHAR (255)   NULL,
    [do_not_phone]                                      BIT              NULL,
    [do_not_phone_flag]                                 CHAR (1)         NULL,
    [do_not_phone_name]                                 NVARCHAR (255)   NULL,
    [do_not_postal_mail_name]                           NVARCHAR (255)   NULL,
    [do_not_send_marketing_material_name]               NVARCHAR (255)   NULL,
    [email_address_1]                                   NVARCHAR (100)   NULL,
    [email_address_2]                                   NVARCHAR (100)   NULL,
    [email_address_3]                                   NVARCHAR (100)   NULL,
    [employee_id]                                       NVARCHAR (50)    NULL,
    [first_name]                                        NVARCHAR (50)    NULL,
    [full_name]                                         NVARCHAR (160)   NULL,
    [gender_code]                                       INT              NULL,
    [gender_code_name]                                  NVARCHAR (255)   NULL,
    [insert_user]                                       VARCHAR (100)    NULL,
    [inserted_date_time]                                DATETIME         NULL,
    [inserted_dim_date_key]                             VARCHAR (8)      NULL,
    [inserted_dim_time_key]                             INT              NULL,
    [last_name]                                         NVARCHAR (50)    NULL,
    [ltf_age]                                           NVARCHAR (3)     NULL,
    [ltf_alternate_full_name]                           NVARCHAR (110)   NULL,
    [ltf_anniversary_call]                              DATETIME         NULL,
    [ltf_anniversary_call_dim_date_key]                 VARCHAR (8)      NULL,
    [ltf_anniversary_call_dim_time_key]                 INT              NULL,
    [ltf_bday_years_difference]                         DATETIME         NULL,
    [ltf_bday_years_difference_dim_date_key]            VARCHAR (8)      NULL,
    [ltf_bday_years_difference_dim_time_key]            INT              NULL,
    [ltf_birth_year]                                    NVARCHAR (4)     NULL,
    [ltf_calculated_age]                                INT              NULL,
    [ltf_club_id_name]                                  NVARCHAR (100)   NULL,
    [ltf_club_proximity]                                INT              NULL,
    [ltf_club_proximity_name]                           NVARCHAR (255)   NULL,
    [ltf_commitment_level]                              INT              NULL,
    [ltf_commitment_level_name]                         NVARCHAR (255)   NULL,
    [ltf_commitment_reason]                             NVARCHAR (4000)  NULL,
    [ltf_connect_member_dim_mms_member_key]             VARCHAR (32)     NULL,
    [ltf_connect_member_id_name]                        NVARCHAR (150)   NULL,
    [ltf_days_since_join_date]                          INT              NULL,
    [ltf_dn_cover_ride_flag]                            CHAR (1)         NULL,
    [ltf_dnc_dne_update_triggered_by]                   INT              NULL,
    [ltf_dnc_dne_update_triggered_by_name]              NVARCHAR (255)   NULL,
    [ltf_dnc_over_ride]                                 BIT              NULL,
    [ltf_dnc_over_ride_name]                            NVARCHAR (255)   NULL,
    [ltf_dnc_temporary_release_expiration]              DATETIME         NULL,
    [ltf_dnc_temporary_release_expiration_dim_date_key] VARCHAR (8)      NULL,
    [ltf_dnc_temporary_release_expiration_dim_time_key] INT              NULL,
    [ltf_do_not_email_address_1]                        BIT              NULL,
    [ltf_do_not_email_address_1_flag]                   CHAR (1)         NULL,
    [ltf_do_not_email_address_1_name]                   NVARCHAR (255)   NULL,
    [ltf_do_not_email_address_2]                        BIT              NULL,
    [ltf_do_not_email_address_2_flag]                   CHAR (1)         NULL,
    [ltf_do_not_email_address_2_name]                   NVARCHAR (255)   NULL,
    [ltf_do_not_phone_mobile_phone]                     BIT              NULL,
    [ltf_do_not_phone_mobile_phone_flag]                CHAR (1)         NULL,
    [ltf_do_not_phone_mobile_phone_name]                NVARCHAR (255)   NULL,
    [ltf_do_not_phone_telephone_1]                      BIT              NULL,
    [ltf_do_not_phone_telephone_1_flag]                 CHAR (1)         NULL,
    [ltf_do_not_phone_telephone_1_name]                 NVARCHAR (255)   NULL,
    [ltf_do_not_phone_telephone_2]                      BIT              NULL,
    [ltf_do_not_phone_telephone_2_flag]                 CHAR (1)         NULL,
    [ltf_do_not_phone_telephone_2_name]                 NVARCHAR (255)   NULL,
    [ltf_duplicate_over_ride]                           BIT              NULL,
    [ltf_duplicate_over_ride_flag]                      CHAR (1)         NULL,
    [ltf_duplicate_over_ride_name]                      NVARCHAR (255)   NULL,
    [ltf_employer_id_name]                              NVARCHAR (160)   NULL,
    [ltf_employer_wellness_program]                     INT              NULL,
    [ltf_employer_wellness_program_name]                NVARCHAR (255)   NULL,
    [ltf_exercise_history]                              INT              NULL,
    [ltf_exercise_history_name]                         NVARCHAR (255)   NULL,
    [ltf_injuries_or_limitations]                       BIT              NULL,
    [ltf_injuries_or_limitations_description]           NVARCHAR (4000)  NULL,
    [ltf_injuries_or_limitations_flag]                  CHAR (1)         NULL,
    [ltf_injuries_or_limitations_name]                  NVARCHAR (255)   NULL,
    [ltf_inserted_by_system]                            BIT              NULL,
    [ltf_inserted_by_system_flag]                       CHAR (1)         NULL,
    [ltf_inserted_by_system_name]                       NVARCHAR (255)   NULL,
    [ltf_is_employee]                                   BIT              NULL,
    [ltf_is_employee_flag]                              CHAR (1)         NULL,
    [ltf_is_employee_name]                              NVARCHAR (255)   NULL,
    [ltf_is_life_time_close_to]                         INT              NULL,
    [ltf_is_life_time_close_to_name]                    NVARCHAR (255)   NULL,
    [ltf_join_date]                                     DATETIME         NULL,
    [ltf_join_dim_date_key]                             VARCHAR (8)      NULL,
    [ltf_join_dim_time_key]                             INT              NULL,
    [ltf_last_contacted_by]                             VARCHAR (36)     NULL,
    [ltf_last_contacted_by_name]                        NVARCHAR (200)   NULL,
    [ltf_lead_source]                                   INT              NULL,
    [ltf_lead_source_name]                              NVARCHAR (255)   NULL,
    [ltf_lead_type]                                     INT              NULL,
    [ltf_lead_type_name]                                NVARCHAR (255)   NULL,
    [ltf_legacy]                                        NVARCHAR (4000)  NULL,
    [ltf_lt_bucks]                                      DECIMAL (18, 10) NULL,
    [ltf_measurable_goal]                               INT              NULL,
    [ltf_measurable_goal_name]                          NVARCHAR (255)   NULL,
    [ltf_member_type_list]                              INT              NULL,
    [ltf_member_type_list_name]                         NVARCHAR (255)   NULL,
    [ltf_most_recent_member_dim_mms_member_key]         VARCHAR (32)     NULL,
    [ltf_nugget]                                        NVARCHAR (4000)  NULL,
    [ltf_past_trainer_or_coach]                         BIT              NULL,
    [ltf_past_trainer_or_coach_flag]                    CHAR (1)         NULL,
    [ltf_past_trainer_or_coach_name]                    NVARCHAR (255)   NULL,
    [ltf_primary_objective]                             INT              NULL,
    [ltf_primary_objective_name]                        NVARCHAR (255)   NULL,
    [ltf_referring_contact_dim_contact_key]             VARCHAR (32)     NULL,
    [ltf_referring_contact_id_name]                     NVARCHAR (160)   NULL,
    [ltf_referring_contact_id_yomi_name]                NVARCHAR (160)   NULL,
    [ltf_risk_score]                                    INT              NULL,
    [ltf_specific_goal]                                 INT              NULL,
    [ltf_specific_goal_name]                            NVARCHAR (255)   NULL,
    [ltf_star_value]                                    INT              NULL,
    [ltf_time_goal]                                     DATETIME         NULL,
    [ltf_time_goal_dim_date_key]                        VARCHAR (8)      NULL,
    [ltf_time_goal_dim_time_key]                        INT              NULL,
    [ltf_todays_action]                                 INT              NULL,
    [ltf_todays_action_name]                            NVARCHAR (255)   NULL,
    [ltf_trainer_or_coach_preference]                   INT              NULL,
    [ltf_trainer_or_coach_preference_name]              NVARCHAR (255)   NULL,
    [ltf_udw_id]                                        NVARCHAR (255)   NULL,
    [ltf_volatile_contact]                              BIT              NULL,
    [ltf_volatile_contact_flag]                         CHAR (1)         NULL,
    [ltf_volatile_contact_name]                         NVARCHAR (255)   NULL,
    [ltf_workout_preference]                            INT              NULL,
    [ltf_workout_preference_name]                       NVARCHAR (255)   NULL,
    [ltf_years_of_membership]                           INT              NULL,
    [middle_name]                                       NVARCHAR (50)    NULL,
    [mobile_phone]                                      NVARCHAR (50)    NULL,
    [modified_by_dim_crm_system_user_key]               VARCHAR (32)     NULL,
    [modified_dim_date_key]                             VARCHAR (8)      NULL,
    [modified_dim_time_key]                             INT              NULL,
    [modified_on]                                       DATETIME         NULL,
    [modified_on_behalf_by_dim_crm_system_user_key]     VARCHAR (32)     NULL,
    [originating_lead_id]                               VARCHAR (36)     NULL,
    [owner_id_name]                                     NVARCHAR (200)   NULL,
    [owner_id_type]                                     NVARCHAR (64)    NULL,
    [owning_business_unit]                              VARCHAR (36)     NULL,
    [owning_user_dim_crm_system_user_key]               VARCHAR (32)     NULL,
    [salutation]                                        NVARCHAR (100)   NULL,
    [state_code]                                        INT              NULL,
    [state_code_name]                                   NVARCHAR (255)   NULL,
    [telephone_1]                                       NVARCHAR (50)    NULL,
    [telephone_2]                                       NVARCHAR (50)    NULL,
    [update_user]                                       VARCHAR (50)     NULL,
    [p_crmcloudsync_contact_id]                         BIGINT           NOT NULL,
    [deleted_flag]                                      INT              NULL,
    [dv_load_date_time]                                 DATETIME         NULL,
    [dv_load_end_date_time]                             DATETIME         NULL,
    [dv_batch_id]                                       BIGINT           NOT NULL,
    [dv_inserted_date_time]                             DATETIME         NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]                              DATETIME         NULL,
    [dv_update_user]                                    VARCHAR (50)     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));
