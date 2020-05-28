CREATE TABLE [dbo].[d_mms_club_gl_account] (
    [d_mms_club_gl_account_id]              BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [club_gl_account_key]                   CHAR (32)    NULL,
    [club_gl_account_id]                    INT          NULL,
    [club_id]                               INT          NULL,
    [gl_cash_entry_account]                 VARCHAR (4)  NULL,
    [gl_cash_entry_cash_sub_account]        VARCHAR (11) NULL,
    [gl_cash_entry_company_name]            VARCHAR (15) NULL,
    [gl_cash_entry_credit_card_sub_account] VARCHAR (11) NULL,
    [gl_receivables_entry_account]          VARCHAR (4)  NULL,
    [gl_receivables_entry_company_name]     VARCHAR (15) NULL,
    [gl_receivables_entry_sub_account]      VARCHAR (11) NULL,
    [val_currency_code_id]                  TINYINT      NULL,
    [p_mms_club_gl_account_id]              BIGINT       NOT NULL,
    [dv_load_date_time]                     DATETIME     NULL,
    [dv_load_end_date_time]                 DATETIME     NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_club_gl_account]([dv_batch_id] ASC);

