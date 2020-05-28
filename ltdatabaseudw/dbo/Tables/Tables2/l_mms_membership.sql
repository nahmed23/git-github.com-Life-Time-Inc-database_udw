CREATE TABLE [dbo].[l_mms_membership] (
    [l_mms_membership_id]                 BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [membership_id]                       INT          NULL,
    [club_id]                             INT          NULL,
    [purchaser_id]                        INT          NULL,
    [advisor_employee_id]                 INT          NULL,
    [company_id]                          INT          NULL,
    [val_eft_option_id]                   INT          NULL,
    [val_enrollment_type_id]              INT          NULL,
    [val_termination_reason_id]           INT          NULL,
    [membership_type_id]                  INT          NULL,
    [val_membership_status_id]            INT          NULL,
    [val_membership_source_id]            INT          NULL,
    [promotion_id]                        INT          NULL,
    [jr_member_dues_product_id]           INT          NULL,
    [salesforce_prospect_id]              VARCHAR (18) NULL,
    [last_updated_employee_id]            INT          NULL,
    [qualified_sales_promotion_id]        INT          NULL,
    [salesforce_account_id]               VARCHAR (18) NULL,
    [salesforce_opportunity_id]           VARCHAR (18) NULL,
    [val_termination_reason_club_type_id] INT          NULL,
    [crm_opportunity_id]                  VARCHAR (36) NULL,
    [prior_plus_membership_type_id]       INT          NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_r_load_source_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_hash]                             CHAR (32)    NOT NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_membership]
    ON [dbo].[l_mms_membership]([bk_hash] ASC, [l_mms_membership_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_membership]([dv_batch_id] ASC);

