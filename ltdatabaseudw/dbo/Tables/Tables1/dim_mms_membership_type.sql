﻿CREATE TABLE [dbo].[dim_mms_membership_type] (
    [dim_mms_membership_type_key]                           VARCHAR (32)  NULL,
    [membership_type_id]                                    INT           NULL,
    [access_by_price_paid_flag]                             CHAR (1)      NULL,
    [allow_partner_program_flag]                            CHAR (1)      NULL,
    [assess_dues_flag]                                      CHAR (1)      NULL,
    [assess_junior_member_dues_flag]                        CHAR (1)      NULL,
    [attribute_26_and_under_flag]                           CHAR (1)      NULL,
    [attribute_acquisition_flag]                            CHAR (1)      NULL,
    [attribute_attrition_exclusion_flag]                    CHAR (1)      NULL,
    [attribute_auto_ship_flag]                              CHAR (1)      NULL,
    [attribute_corporate_flex_flag]                         CHAR (1)      NULL,
    [attribute_dssr_group_description]                      VARCHAR (50)  NULL,
    [attribute_employee_membership_flag]                    CHAR (1)      NULL,
    [attribute_express_membership_flag]                     CHAR (1)      NULL,
    [attribute_flexible_pass_flag]                          CHAR (1)      NULL,
    [attribute_founders_flag]                               CHAR (1)      NULL,
    [attribute_free_magazine_flag]                          CHAR (1)      NULL,
    [attribute_junior_members_not_allowed_flag]             CHAR (1)      NULL,
    [attribute_life_time_health_flag]                       CHAR (1)      NULL,
    [attribute_membership_status_summary_group_description] VARCHAR (100) NULL,
    [attribute_my_health_check_flag]                        CHAR (1)      NULL,
    [attribute_non_access_flag]                             CHAR (1)      NULL,
    [attribute_not_eligible_for_magazine_flag]              CHAR (1)      NULL,
    [attribute_pending_non_access_flag]                     CHAR (1)      NULL,
    [attribute_short_term_membership_flag]                  CHAR (1)      NULL,
    [attribute_student_flex_flag]                           CHAR (1)      NULL,
    [attribute_trade_out_membership_flag]                   CHAR (1)      NULL,
    [attribute_vip_flag]                                    CHAR (1)      NULL,
    [check_in_group_description]                            VARCHAR (50)  NULL,
    [dim_mms_product_key]                                   VARCHAR (32)  NULL,
    [enrollment_type_description]                           VARCHAR (50)  NULL,
    [express_membership_flag]                               CHAR (1)      NULL,
    [family_status_description]                             VARCHAR (50)  NULL,
    [gta_signature_override]                                VARCHAR (100) NULL,
    [membership_type]                                       VARCHAR (50)  NULL,
    [membership_type_group_description]                     VARCHAR (50)  NULL,
    [pricing_method_description]                            VARCHAR (50)  NULL,
    [pricing_rule_description]                              VARCHAR (50)  NULL,
    [primary_age_minimum]                                   INT           NULL,
    [product_description]                                   VARCHAR (50)  NULL,
    [product_house_account_flag]                            CHAR (1)      NULL,
    [product_id]                                            INT           NULL,
    [product_investor_flag]                                 CHAR (1)      NULL,
    [restricted_group_description]                          VARCHAR (50)  NULL,
    [short_term_membership_flag]                            CHAR (1)      NULL,
    [suppress_membership_card_flag]                         CHAR (1)      NULL,
    [unit_type_description]                                 VARCHAR (50)  NULL,
    [unit_type_maximum]                                     INT           NULL,
    [unit_type_minimum]                                     INT           NULL,
    [val_check_in_group_id]                                 INT           NULL,
    [val_enrollment_type_id]                                INT           NULL,
    [val_membership_type_family_status_id]                  INT           NULL,
    [val_membership_type_group_id]                          INT           NULL,
    [val_pricing_method_id]                                 INT           NULL,
    [val_pricing_rule_id]                                   INT           NULL,
    [val_restricted_group_id]                               INT           NULL,
    [val_unit_type_id]                                      INT           NULL,
    [val_welcome_kit_type_id]                               INT           NULL,
    [waive_admin_fee_flag]                                  CHAR (1)      NULL,
    [waive_enrollment_fee_flag]                             CHAR (1)      NULL,
    [waive_late_fee_flag]                                   CHAR (1)      NULL,
    [welcome_kit_type_description]                          VARCHAR (50)  NULL,
    [p_mms_membership_type_id]                              BIGINT        NULL,
    [dv_load_date_time]                                     DATETIME      NULL,
    [dv_load_end_date_time]                                 DATETIME      NULL,
    [dv_batch_id]                                           BIGINT        NULL,
    [dv_inserted_date_time]                                 DATETIME      NOT NULL,
    [dv_insert_user]                                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                                  DATETIME      NULL,
    [dv_update_user]                                        VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

