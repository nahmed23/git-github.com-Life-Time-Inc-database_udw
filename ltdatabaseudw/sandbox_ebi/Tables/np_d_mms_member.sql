﻿CREATE TABLE [sandbox_ebi].[np_d_mms_member] (
    [d_mms_member_id]                 BIGINT         NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [raw_member_id]                   INT            NULL,
    [raw_membership_id]               INT            NULL,
    [raw_employer_id]                 INT            NULL,
    [raw_first_name]                  VARCHAR (50)   NULL,
    [raw_middle_name]                 VARCHAR (25)   NULL,
    [raw_last_name]                   VARCHAR (50)   NULL,
    [raw_dob]                         DATETIME       NULL,
    [raw_gender]                      CHAR (1)       NULL,
    [raw_active_flag]                 BIT            NULL,
    [raw_has_message_flag]            BIT            NULL,
    [raw_join_date]                   DATETIME       NULL,
    [raw_comment]                     VARCHAR (250)  NULL,
    [raw_val_member_type_id]          INT            NULL,
    [raw_inserted_date_time]          DATETIME       NULL,
    [raw_val_name_prefix_id]          INT            NULL,
    [raw_val_name_suffix_id]          INT            NULL,
    [raw_email_address]               VARCHAR (140)  NULL,
    [raw_credit_card_account_id]      INT            NULL,
    [raw_charge_to_account_flag]      BIT            NULL,
    [raw_cw_medica_number]            VARCHAR (16)   NULL,
    [raw_cw_enrollment_date]          DATETIME       NULL,
    [raw_cw_program_enrolled_flag]    BIT            NULL,
    [raw_mip_updated_date_time]       DATETIME       NULL,
    [raw_siebel_row_id]               VARCHAR (15)   NULL,
    [raw_updated_date_time]           DATETIME       NULL,
    [raw_photo_delete_date_time]      DATETIME       NULL,
    [raw_salesforce_prospect_id]      VARCHAR (18)   NULL,
    [raw_member_token]                BINARY (20)    NULL,
    [raw_party_id]                    INT            NULL,
    [raw_last_updated_employee_id]    INT            NULL,
    [raw_salesforce_contact_id]       VARCHAR (18)   NULL,
    [raw_assess_jr_member_dues_flag]  BIT            NULL,
    [raw_crm_contact_id]              VARCHAR (36)   NULL,
    [dim_mms_member_key]              CHAR (32)      NOT NULL,
    [assess_junior_member_dues_flag]  VARCHAR (1)    NOT NULL,
    [customer_name]                   VARCHAR (101)  NULL,
    [customer_name_last_first]        VARCHAR (102)  NULL,
    [date_of_birth]                   DATETIME       NULL,
    [description_member]              VARCHAR (50)   NOT NULL,
    [dim_mms_membership_key]          CHAR (32)      NULL,
    [email_address]                   VARCHAR (140)  NOT NULL,
    [first_name]                      VARCHAR (50)   NOT NULL,
    [gender_abbreviation]             VARCHAR (1)    NULL,
    [join_date]                       DATETIME       NULL,
    [join_date_key]                   VARCHAR (32)   NULL,
    [last_name]                       VARCHAR (50)   NOT NULL,
    [member_active_flag]              VARCHAR (1)    NOT NULL,
    [member_type_dim_description_key] VARCHAR (54)   NULL,
    [dv_batch_id]                     BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  NVARCHAR (128) NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_first_in_key_series_flag]     VARCHAR (1)    NOT NULL,
    [dv_effective_date_time]          DATETIME       NULL,
    [dv_expiration_date_time]         DATETIME       NOT NULL,
    [dv_deleted_flag]                 CHAR (1)       NULL,
    [dv_source_hash]                  CHAR (32)      NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

