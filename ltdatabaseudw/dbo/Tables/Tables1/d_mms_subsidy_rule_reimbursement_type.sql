CREATE TABLE [dbo].[d_mms_subsidy_rule_reimbursement_type] (
    [d_mms_subsidy_rule_reimbursement_type_id]                        BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                         CHAR (32)      NOT NULL,
    [dim_mms_subsidy_rule_reimbursement_type_key]                     CHAR (32)      NULL,
    [subsidy_rule_reimbursement_type_id]                              INT            NULL,
    [subsidy_reimbursement_couple_membership_dues_amount]             DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_couple_membership_dues_flag]               CHAR (1)       NULL,
    [subsidy_reimbursement_couple_membership_dues_include_tax_flag]   CHAR (1)       NULL,
    [subsidy_reimbursement_couple_membership_dues_percentage]         DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_experience_life_magazine_amount]           DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_experience_life_magazine_flag]             CHAR (1)       NULL,
    [subsidy_reimbursement_experience_life_magazine_include_tax_flag] CHAR (1)       NULL,
    [subsidy_reimbursement_experience_life_magazine_percentage]       DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_family_membership_dues_amount]             DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_family_membership_dues_flag]               CHAR (1)       NULL,
    [subsidy_reimbursement_family_membership_dues_include_tax_flag]   CHAR (1)       NULL,
    [subsidy_reimbursement_family_membership_dues_percentage]         DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_junior_member_dues_amount]                 DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_junior_member_dues_flag]                   CHAR (1)       NULL,
    [subsidy_reimbursement_junior_member_dues_include_tax_flag]       CHAR (1)       NULL,
    [subsidy_reimbursement_junior_member_dues_percentage]             DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_single_membership_dues_amount]             DECIMAL (6, 2) NULL,
    [subsidy_reimbursement_single_membership_dues_flag]               CHAR (1)       NULL,
    [subsidy_reimbursement_single_membership_dues_include_tax_flag]   CHAR (1)       NULL,
    [subsidy_reimbursement_single_membership_dues_percentage]         DECIMAL (6, 2) NULL,
    [subsidy_rule_id]                                                 INT            NULL,
    [val_reimbursement_type_id]                                       INT            NULL,
    [deleted_flag]                                                    INT            NULL,
    [p_mms_subsidy_rule_reimbursement_type_id]                        BIGINT         NOT NULL,
    [dv_load_date_time]                                               DATETIME       NULL,
    [dv_load_end_date_time]                                           DATETIME       NULL,
    [dv_batch_id]                                                     BIGINT         NOT NULL,
    [dv_inserted_date_time]                                           DATETIME       NOT NULL,
    [dv_insert_user]                                                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                                            DATETIME       NULL,
    [dv_update_user]                                                  VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_subsidy_rule_reimbursement_type]([dv_batch_id] ASC);

