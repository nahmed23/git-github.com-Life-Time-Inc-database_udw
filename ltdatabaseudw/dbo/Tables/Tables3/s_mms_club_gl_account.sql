CREATE TABLE [dbo].[s_mms_club_gl_account] (
    [s_mms_club_gl_account_id]              BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [club_gl_account_id]                    INT          NULL,
    [gl_cash_entry_company_name]            VARCHAR (15) NULL,
    [gl_cash_entry_account]                 VARCHAR (4)  NULL,
    [gl_receivables_entry_account]          VARCHAR (4)  NULL,
    [gl_cash_entry_cash_sub_account]        VARCHAR (11) NULL,
    [gl_cash_entry_credit_card_sub_account] VARCHAR (11) NULL,
    [gl_receivables_entry_sub_account]      VARCHAR (11) NULL,
    [gl_receivables_entry_company_name]     VARCHAR (15) NULL,
    [inserted_date_time]                    DATETIME     NULL,
    [updated_date_time]                     DATETIME     NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_r_load_source_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_hash]                               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_club_gl_account]
    ON [dbo].[s_mms_club_gl_account]([bk_hash] ASC, [s_mms_club_gl_account_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_club_gl_account]([dv_batch_id] ASC);

