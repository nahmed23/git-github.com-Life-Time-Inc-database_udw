CREATE TABLE [dbo].[d_mms_company] (
    [d_mms_company_id]                BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [dim_mms_company_key]             CHAR (32)     NULL,
    [company_id]                      INT           NULL,
    [account_rep_name]                VARCHAR (50)  NULL,
    [company_name]                    VARCHAR (50)  NULL,
    [corporate_code]                  VARCHAR (50)  NULL,
    [eft_account_number_on_file_flag] CHAR (1)      NULL,
    [invoice_flag]                    CHAR (1)      NULL,
    [report_to_email_address]         VARCHAR (150) NULL,
    [small_business_flag]             CHAR (1)      NULL,
    [usage_report_flag]               CHAR (1)      NULL,
    [usage_report_member_type]        VARCHAR (50)  NULL,
    [p_mms_company_id]                BIGINT        NOT NULL,
    [dv_load_date_time]               DATETIME      NULL,
    [dv_load_end_date_time]           DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_company]([dv_batch_id] ASC);

