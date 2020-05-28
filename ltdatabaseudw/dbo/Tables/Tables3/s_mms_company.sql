CREATE TABLE [dbo].[s_mms_company] (
    [s_mms_company_id]               BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)      NOT NULL,
    [company_id]                     INT            NULL,
    [account_rep_initials]           VARCHAR (5)    NULL,
    [company_name]                   VARCHAR (50)   NULL,
    [print_usage_report_flag]        BIT            NULL,
    [corporate_code]                 VARCHAR (50)   NULL,
    [inserted_date_time]             DATETIME       NULL,
    [start_date]                     DATETIME       NULL,
    [end_date]                       DATETIME       NULL,
    [account_rep_name]               VARCHAR (50)   NULL,
    [initiation_fee]                 NUMERIC (8, 4) NULL,
    [updated_date_time]              DATETIME       NULL,
    [enrollment_disc_percentage]     NUMERIC (7, 4) NULL,
    [mac_enrollment_disc_percentage] NUMERIC (7, 4) NULL,
    [invoice_flag]                   BIT            NULL,
    [dollar_discount]                NUMERIC (8, 4) NULL,
    [admin_fee]                      NUMERIC (8, 4) NULL,
    [override_percentage]            NUMERIC (7, 4) NULL,
    [eft_account_number]             VARCHAR (4)    NULL,
    [usage_report_flag]              BIT            NULL,
    [report_to_email_address]        VARCHAR (150)  NULL,
    [usage_report_member_type]       VARCHAR (50)   NULL,
    [small_business_flag]            BIT            NULL,
    [account_owner]                  VARCHAR (100)  NULL,
    [subsidy_measurement]            VARCHAR (30)   NULL,
    [opportunity_record_type]        VARCHAR (50)   NULL,
    [dv_load_date_time]              DATETIME       NOT NULL,
    [dv_r_load_source_id]            BIGINT         NOT NULL,
    [dv_inserted_date_time]          DATETIME       NOT NULL,
    [dv_insert_user]                 VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]           DATETIME       NULL,
    [dv_update_user]                 VARCHAR (50)   NULL,
    [dv_hash]                        CHAR (32)      NOT NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_company]
    ON [dbo].[s_mms_company]([bk_hash] ASC, [s_mms_company_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_company]([dv_batch_id] ASC);

