CREATE TABLE [dbo].[dim_club_currency_code] (
    [dim_club_currency_code_id]             BIGINT       IDENTITY (1, 1) NOT NULL,
    [dummy_bk_hash_key]                     CHAR (32)    NULL,
    [club_id]                               INT          NULL,
    [currency_code]                         VARCHAR (12) NULL,
    [dim_club_currency_code_key]            CHAR (32)    NULL,
    [dim_club_key]                          CHAR (32)    NULL,
    [gl_cash_entry_account]                 VARCHAR (12) NULL,
    [gl_cash_entry_cash_sub_account]        VARCHAR (12) NULL,
    [gl_cash_entry_company_name]            VARCHAR (15) NULL,
    [gl_cash_entry_credit_card_sub_account] VARCHAR (12) NULL,
    [gl_receivables_entry_account]          VARCHAR (12) NULL,
    [gl_receivables_entry_company_name]     VARCHAR (15) NULL,
    [gl_receivables_entry_sub_account]      VARCHAR (12) NULL,
    [dv_load_date_time]                     DATETIME     NULL,
    [dv_load_end_date_time]                 DATETIME     NULL,
    [dv_batch_id]                           BIGINT       NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dummy_bk_hash_key]));

