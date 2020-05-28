CREATE TABLE [dbo].[s_mms_membership_balance] (
    [s_mms_membership_balance_id]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [membership_id]                           INT             NULL,
    [current_balance]                         DECIMAL (26, 6) NULL,
    [eft_amount]                              DECIMAL (26, 6) NULL,
    [statement_balance]                       DECIMAL (26, 6) NULL,
    [assessed_date_time]                      DATETIME        NULL,
    [statement_date_time]                     DATETIME        NULL,
    [previous_statement_balance]              DECIMAL (26, 6) NULL,
    [previous_statement_datetime]             DATETIME        NULL,
    [committed_balance]                       DECIMAL (26, 6) NULL,
    [inserted_date_time]                      DATETIME        NULL,
    [updated_date_time]                       DATETIME        NULL,
    [resubmit_collect_from_bank_account_flag] BIT             NULL,
    [committed_balance_products]              DECIMAL (26, 6) NULL,
    [current_balance_products]                DECIMAL (26, 6) NULL,
    [eft_amount_products]                     DECIMAL (26, 6) NULL,
    [dv_load_date_time]                       DATETIME        NOT NULL,
    [dv_r_load_source_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL,
    [dv_hash]                                 CHAR (32)       NOT NULL,
    [dv_batch_id]                             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_balance]
    ON [dbo].[s_mms_membership_balance]([bk_hash] ASC, [s_mms_membership_balance_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_balance]([dv_batch_id] ASC);

