﻿CREATE TABLE [dbo].[d_crmcloudsync_ltf_outreach_member_summary] (
    [d_crmcloudsync_ltf_outreach_member_summary_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)      NOT NULL,
    [dim_crm_ltf_outreach_member_summary_key]          VARCHAR (32)   NULL,
    [ltf_outreach_member_summary_id]                   VARCHAR (36)   NULL,
    [activation_dim_date_key]                          VARCHAR (8)    NULL,
    [activation_dim_time_key]                          INT            NULL,
    [claim_expiration_dim_date_key]                    VARCHAR (8)    NULL,
    [claim_expiration_dim_time_key]                    INT            NULL,
    [dim_crm_ltf_program_cycle_reference_key]          VARCHAR (32)   NULL,
    [dim_crm_ltf_subscription_key]                     VARCHAR (32)   NULL,
    [dim_crm_owner_key]                                VARCHAR (32)   NULL,
    [dim_crm_product_key]                              VARCHAR (32)   NULL,
    [initial_contact_cycle_dim_date_key]               VARCHAR (8)    NULL,
    [initial_contact_cycle_dim_time_key]               INT            NULL,
    [insert_user]                                      VARCHAR (100)  NULL,
    [inserted_date_time]                               DATETIME       NULL,
    [inserted_dim_date_key]                            VARCHAR (8)    NULL,
    [inserted_dim_time_key]                            INT            NULL,
    [last_attempt_dim_date_key]                        VARCHAR (8)    NULL,
    [last_attempt_dim_time_key]                        INT            NULL,
    [last_contact_dim_date_key]                        VARCHAR (8)    NULL,
    [last_contact_dim_time_key]                        INT            NULL,
    [ltf_activation_date]                              DATETIME       NULL,
    [ltf_claim_expiration]                             DATETIME       NULL,
    [ltf_claimed_by_dim_crm_system_user_key]           VARCHAR (32)   NULL,
    [ltf_claimed_by_name]                              NVARCHAR (200) NULL,
    [ltf_contact_dim_crm_contact_key]                  VARCHAR (32)   NULL,
    [ltf_contact_name]                                 NVARCHAR (160) NULL,
    [ltf_description]                                  NVARCHAR (250) NULL,
    [ltf_enrolled_by_dim_crm_system_user_key]          VARCHAR (32)   NULL,
    [ltf_enrolled_by_name]                             NVARCHAR (200) NULL,
    [ltf_initial_contact_by_cycle_dim_crm_contact_key] VARCHAR (32)   NULL,
    [ltf_initial_contact_by_cycle_name]                NVARCHAR (200) NULL,
    [ltf_initial_contact_date_cycle]                   DATETIME       NULL,
    [ltf_initial_contact_type_cycle]                   INT            NULL,
    [ltf_initial_contact_type_cycle_name]              NVARCHAR (255) NULL,
    [ltf_intercept_attempts_cycle]                     INT            NULL,
    [ltf_intercept_contacts_cycle]                     INT            NULL,
    [ltf_last_attempt_by_dim_crm_system_user_key]      VARCHAR (32)   NULL,
    [ltf_last_attempt_by_name]                         NVARCHAR (200) NULL,
    [ltf_last_attempt_date]                            DATETIME       NULL,
    [ltf_last_attempt_type]                            INT            NULL,
    [ltf_last_attempt_type_name]                       NVARCHAR (255) NULL,
    [ltf_last_contact_by_dim_crm_system_user_key]      VARCHAR (32)   NULL,
    [ltf_last_contact_by_name]                         NVARCHAR (200) NULL,
    [ltf_last_contact_date]                            DATETIME       NULL,
    [ltf_last_contact_type]                            INT            NULL,
    [ltf_last_contact_type_name]                       NVARCHAR (255) NULL,
    [ltf_ltf_employee]                                 INT            NULL,
    [ltf_ltf_employee_name]                            NVARCHAR (255) NULL,
    [ltf_lthealth]                                     INT            NULL,
    [ltf_lthealth_name]                                NVARCHAR (255) NULL,
    [ltf_meeting_attempts_cycle]                       INT            NULL,
    [ltf_meeting_contacts_cycle]                       INT            NULL,
    [ltf_member_number]                                NVARCHAR (100) NULL,
    [ltf_membership_product]                           NVARCHAR (250) NULL,
    [ltf_next_anniversary]                             DATETIME       NULL,
    [ltf_outreach_rank]                                INT            NULL,
    [ltf_phone_attempts_cycle]                         INT            NULL,
    [ltf_phone_contacts_cycle]                         INT            NULL,
    [ltf_product_name]                                 NVARCHAR (100) NULL,
    [ltf_program_cycle_reference_name]                 NVARCHAR (100) NULL,
    [ltf_risk_score]                                   INT            NULL,
    [ltf_role]                                         INT            NULL,
    [ltf_role_name]                                    NVARCHAR (255) NULL,
    [ltf_segment]                                      INT            NULL,
    [ltf_segment_name]                                 NVARCHAR (255) NULL,
    [ltf_star_value]                                   INT            NULL,
    [ltf_subscription_id_name]                         NVARCHAR (100) NULL,
    [ltf_subsegment_1]                                 INT            NULL,
    [ltf_subsegment_1_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_10]                                INT            NULL,
    [ltf_subsegment_10_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_11]                                INT            NULL,
    [ltf_subsegment_11_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_12]                                INT            NULL,
    [ltf_subsegment_12_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_13]                                INT            NULL,
    [ltf_subsegment_13_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_14]                                INT            NULL,
    [ltf_subsegment_14_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_15]                                INT            NULL,
    [ltf_subsegment_15_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_16]                                INT            NULL,
    [ltf_subsegment_16_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_17]                                INT            NULL,
    [ltf_subsegment_17_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_18]                                INT            NULL,
    [ltf_subsegment_18_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_19]                                INT            NULL,
    [ltf_subsegment_19_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_2]                                 INT            NULL,
    [ltf_subsegment_2_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_20]                                INT            NULL,
    [ltf_subsegment_20_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_21]                                INT            NULL,
    [ltf_subsegment_21_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_22]                                INT            NULL,
    [ltf_subsegment_22_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_23]                                INT            NULL,
    [ltf_subsegment_23_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_24]                                INT            NULL,
    [ltf_subsegment_24_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_25]                                INT            NULL,
    [ltf_subsegment_25_name]                           NVARCHAR (255) NULL,
    [ltf_subsegment_3]                                 INT            NULL,
    [ltf_subsegment_3_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_4]                                 INT            NULL,
    [ltf_subsegment_4_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_5]                                 INT            NULL,
    [ltf_subsegment_5_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_6]                                 INT            NULL,
    [ltf_subsegment_6_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_7]                                 INT            NULL,
    [ltf_subsegment_7_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_8]                                 INT            NULL,
    [ltf_subsegment_8_name]                            NVARCHAR (255) NULL,
    [ltf_subsegment_9]                                 INT            NULL,
    [ltf_subsegment_9_name]                            NVARCHAR (255) NULL,
    [ltf_talking_points]                               VARCHAR (8000) NULL,
    [ltf_targeted]                                     BIT            NULL,
    [ltf_targeted_name]                                NVARCHAR (255) NULL,
    [ltf_total_attempts_cycle]                         INT            NULL,
    [ltf_total_contacts_cycle]                         INT            NULL,
    [ltf_years_of_membership]                          INT            NULL,
    [modified_by_dim_crm_system_user_key]              VARCHAR (32)   NULL,
    [modified_by_name]                                 NVARCHAR (200) NULL,
    [modified_dim_date_key]                            VARCHAR (8)    NULL,
    [modified_dim_time_key]                            INT            NULL,
    [modified_on]                                      DATETIME       NULL,
    [modified_on_behalf_by_dim_crm_system_user_key]    VARCHAR (32)   NULL,
    [modified_on_behalf_by_name]                       NVARCHAR (200) NULL,
    [next_anniversary_dim_date_key]                    VARCHAR (8)    NULL,
    [next_anniversary_dim_time_key]                    INT            NULL,
    [owner_id_name]                                    NVARCHAR (200) NULL,
    [owner_id_type]                                    NVARCHAR (64)  NULL,
    [owning_business_unit]                             VARCHAR (36)   NULL,
    [owning_user_dim_crm_system_user_key]              VARCHAR (32)   NULL,
    [state_code]                                       INT            NULL,
    [state_code_name]                                  NVARCHAR (255) NULL,
    [status_code]                                      INT            NULL,
    [status_code_name]                                 NVARCHAR (255) NULL,
    [targeted_flag]                                    CHAR (1)       NULL,
    [time_zone_rule_version_number]                    INT            NULL,
    [update_user]                                      VARCHAR (50)   NULL,
    [updated_date_time]                                DATETIME       NULL,
    [updated_dim_date_key]                             VARCHAR (8)    NULL,
    [updated_dim_time_key]                             INT            NULL,
    [version_number]                                   BIGINT         NULL,
    [p_crmcloudsync_ltf_outreach_member_summary_id]    BIGINT         NOT NULL,
    [deleted_flag]                                     INT            NULL,
    [dv_load_date_time]                                DATETIME       NULL,
    [dv_load_end_date_time]                            DATETIME       NULL,
    [dv_batch_id]                                      BIGINT         NOT NULL,
    [dv_inserted_date_time]                            DATETIME       NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                             DATETIME       NULL,
    [dv_update_user]                                   VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

