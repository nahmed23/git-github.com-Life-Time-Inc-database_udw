CREATE TABLE [dbo].[d_mms_subsidy_company_reimbursement_program] (
    [d_mms_subsidy_company_reimbursement_program_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)     NOT NULL,
    [dim_mms_subsidy_company_reimbursement_program_key] CHAR (32)     NULL,
    [subsidy_company_reimbursement_program_id]          INT           NULL,
    [reimbursement_program_id]                          INT           NULL,
    [subsidy_program_description]                       VARCHAR (255) NULL,
    [subsidy_program_flag]                              CHAR (1)      NULL,
    [deleted_flag]                                      INT           NULL,
    [p_mms_subsidy_company_reimbursement_program_id]    BIGINT        NOT NULL,
    [dv_load_date_time]                                 DATETIME      NULL,
    [dv_load_end_date_time]                             DATETIME      NULL,
    [dv_batch_id]                                       BIGINT        NOT NULL,
    [dv_inserted_date_time]                             DATETIME      NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                              DATETIME      NULL,
    [dv_update_user]                                    VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_subsidy_company_reimbursement_program]([dv_batch_id] ASC);

