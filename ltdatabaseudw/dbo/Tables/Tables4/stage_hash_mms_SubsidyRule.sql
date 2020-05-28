CREATE TABLE [dbo].[stage_hash_mms_SubsidyRule] (
    [stage_hash_mms_SubsidyRule_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)      NOT NULL,
    [SubsidyRuleID]                           INT            NULL,
    [SubsidyCompanyReimbursementProgramID]    INT            NULL,
    [ValReimbursementUsageTypeID]             TINYINT        NULL,
    [Description]                             VARCHAR (255)  NULL,
    [UsageMinimum]                            SMALLINT       NULL,
    [MaxVisitsPerDay]                         SMALLINT       NULL,
    [ReimbursementAmountPerUsage]             NUMERIC (6, 2) NULL,
    [IgnoreUsageMinimumFirstMonthFlag]        BIT            NULL,
    [IncludeTaxUsageTierFlag]                 BIT            NULL,
    [InsertedDateTime]                        DATETIME       NULL,
    [UpdatedDateTime]                         DATETIME       NULL,
    [IgnoreUsageMinimumPreviousNonAccessFlag] BIT            NULL,
    [ApplyUsageCreditsPreviousAccessFlag]     BIT            NULL,
    [dv_load_date_time]                       DATETIME       NOT NULL,
    [dv_inserted_date_time]                   DATETIME       NOT NULL,
    [dv_insert_user]                          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                    DATETIME       NULL,
    [dv_update_user]                          VARCHAR (50)   NULL,
    [dv_batch_id]                             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

