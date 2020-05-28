﻿CREATE TABLE [dbo].[l_mms_subsidy_rule] (
    [l_mms_subsidy_rule_id]                    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [subsidy_rule_id]                          INT          NULL,
    [subsidy_company_reimbursement_program_id] INT          NULL,
    [val_reimbursement_usage_type_id]          TINYINT      NULL,
    [dv_load_date_time]                        DATETIME     NOT NULL,
    [dv_batch_id]                              BIGINT       NOT NULL,
    [dv_r_load_source_id]                      BIGINT       NOT NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL,
    [dv_hash]                                  CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_subsidy_rule]
    ON [dbo].[l_mms_subsidy_rule]([bk_hash] ASC, [l_mms_subsidy_rule_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_subsidy_rule]([dv_batch_id] ASC);

