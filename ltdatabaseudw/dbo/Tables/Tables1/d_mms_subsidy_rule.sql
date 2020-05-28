CREATE TABLE [dbo].[d_mms_subsidy_rule] (
    [d_mms_subsidy_rule_id]                                BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                              CHAR (32)     NOT NULL,
    [dim_mms_subsidy_rule_key]                             CHAR (32)     NULL,
    [subsidy_rule_id]                                      INT           NULL,
    [subsidy_company_reimbursement_program_id]             INT           NULL,
    [subsidy_reimbursement_usage_type_dim_description_key] VARCHAR (200) NULL,
    [deleted_flag]                                         INT           NULL,
    [p_mms_subsidy_rule_id]                                BIGINT        NOT NULL,
    [dv_load_date_time]                                    DATETIME      NULL,
    [dv_load_end_date_time]                                DATETIME      NULL,
    [dv_batch_id]                                          BIGINT        NOT NULL,
    [dv_inserted_date_time]                                DATETIME      NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                                 DATETIME      NULL,
    [dv_update_user]                                       VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_subsidy_rule]([dv_batch_id] ASC);

