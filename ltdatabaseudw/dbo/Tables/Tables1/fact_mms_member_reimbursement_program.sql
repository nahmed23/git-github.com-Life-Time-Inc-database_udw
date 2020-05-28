CREATE TABLE [dbo].[fact_mms_member_reimbursement_program] (
    [fact_mms_member_reimbursement_program_id]   BIGINT        IDENTITY (1, 1) NOT NULL,
    [fact_mms_member_reimbursement_program_key]  CHAR (32)     NULL,
    [dim_mms_company_key]                        CHAR (32)     NULL,
    [dim_mms_member_key]                         CHAR (32)     NULL,
    [dim_mms_membership_key]                     CHAR (32)     NULL,
    [dim_mms_reimbursement_program_key]          CHAR (32)     NULL,
    [enrollment_date]                            DATETIME      NULL,
    [enrollment_dim_date_key]                    CHAR (8)      NULL,
    [identifier_field1_name_dim_description_key] VARCHAR (150) NULL,
    [identifier_field1_value]                    CHAR (100)    NULL,
    [identifier_field2_name_dim_description_key] VARCHAR (150) NULL,
    [identifier_field2_value]                    CHAR (100)    NULL,
    [identifier_field3_name_dim_description_key] VARCHAR (150) NULL,
    [identifier_field3_value]                    CHAR (100)    NULL,
    [member_reimbursement_id]                    INT           NULL,
    [termination_date]                           DATETIME      NULL,
    [termination_dim_date_key]                   CHAR (8)      NULL,
    [p_mms_member_reimbursement_id]              BIGINT        NULL,
    [dv_load_date_time]                          DATETIME      NULL,
    [dv_load_end_date_time]                      DATETIME      NULL,
    [dv_batch_id]                                BIGINT        NULL,
    [dv_inserted_date_time]                      DATETIME      NOT NULL,
    [dv_insert_user]                             VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                       DATETIME      NULL,
    [dv_update_user]                             VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_member_reimbursement_program_key]));

