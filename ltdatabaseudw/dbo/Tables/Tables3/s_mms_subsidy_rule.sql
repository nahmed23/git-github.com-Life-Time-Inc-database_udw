CREATE TABLE [dbo].[s_mms_subsidy_rule] (
    [s_mms_subsidy_rule_id]                         BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)      NOT NULL,
    [subsidy_rule_id]                               INT            NULL,
    [description]                                   VARCHAR (255)  NULL,
    [usage_minimum]                                 SMALLINT       NULL,
    [max_visits_per_day]                            SMALLINT       NULL,
    [reimbursement_amount_per_usage]                NUMERIC (6, 2) NULL,
    [ignore_usage_minimum_first_month_flag]         BIT            NULL,
    [include_tax_usage_tier_flag]                   BIT            NULL,
    [inserted_date_time]                            DATETIME       NULL,
    [updated_date_time]                             DATETIME       NULL,
    [ignore_usage_minimum_previous_non_access_flag] BIT            NULL,
    [apply_usage_credits_previous_access_flag]      BIT            NULL,
    [dv_load_date_time]                             DATETIME       NOT NULL,
    [dv_batch_id]                                   BIGINT         NOT NULL,
    [dv_r_load_source_id]                           BIGINT         NOT NULL,
    [dv_inserted_date_time]                         DATETIME       NOT NULL,
    [dv_insert_user]                                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                          DATETIME       NULL,
    [dv_update_user]                                VARCHAR (50)   NULL,
    [dv_hash]                                       CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_subsidy_rule]
    ON [dbo].[s_mms_subsidy_rule]([bk_hash] ASC, [s_mms_subsidy_rule_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_subsidy_rule]([dv_batch_id] ASC);

