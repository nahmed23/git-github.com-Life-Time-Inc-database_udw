CREATE TABLE [dbo].[s_mms_subsidy_rule_reimbursement_type] (
    [s_mms_subsidy_rule_reimbursement_type_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)      NOT NULL,
    [subsidy_rule_reimbursement_type_id]       INT            NULL,
    [reimbursement_amount]                     DECIMAL (6, 2) NULL,
    [reimbursement_percentage]                 DECIMAL (6, 2) NULL,
    [include_tax_flag]                         BIT            NULL,
    [inserted_date_time]                       DATETIME       NULL,
    [updated_date_time]                        DATETIME       NULL,
    [dv_load_date_time]                        DATETIME       NOT NULL,
    [dv_batch_id]                              BIGINT         NOT NULL,
    [dv_r_load_source_id]                      BIGINT         NOT NULL,
    [dv_inserted_date_time]                    DATETIME       NOT NULL,
    [dv_insert_user]                           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                     DATETIME       NULL,
    [dv_update_user]                           VARCHAR (50)   NULL,
    [dv_hash]                                  CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_subsidy_rule_reimbursement_type]
    ON [dbo].[s_mms_subsidy_rule_reimbursement_type]([bk_hash] ASC, [s_mms_subsidy_rule_reimbursement_type_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_subsidy_rule_reimbursement_type]([dv_batch_id] ASC);

