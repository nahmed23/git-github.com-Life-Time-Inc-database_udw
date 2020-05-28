CREATE TABLE [dbo].[s_mms_ACH_charge_back_detail] (
    [s_mms_ACH_charge_back_detail_id]               BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)       NOT NULL,
    [region_description]                            VARCHAR (50)    NULL,
    [club_name]                                     VARCHAR (50)    NULL,
    [first_name]                                    VARCHAR (50)    NULL,
    [last_name]                                     VARCHAR (50)    NULL,
    [ach_cc]                                        VARCHAR (11)    NULL,
    [payment_type_description]                      VARCHAR (50)    NULL,
    [eft_date]                                      VARCHAR (50)    NULL,
    [reason_code_description]                       VARCHAR (50)    NULL,
    [membership_type_product_description]           VARCHAR (50)    NULL,
    [return_code_description]                       VARCHAR (50)    NULL,
    [eft_return_stop_eft_flag]                      VARCHAR (1)     NULL,
    [eft_return_routing_number]                     VARCHAR (50)    NULL,
    [eft_return_account_number]                     VARCHAR (50)    NULL,
    [eft_return_account_expiration_date]            VARCHAR (50)    NULL,
    [membership_phone]                              VARCHAR (14)    NULL,
    [email_address]                                 VARCHAR (140)   NULL,
    [charge_back_post_date_time]                    VARCHAR (50)    NULL,
    [charge_back_membership_eft_option_description] VARCHAR (50)    NULL,
    [charge_back_mms_tran_id]                       INT             NULL,
    [charge_back_tran_amount]                       DECIMAL (14, 4) NULL,
    [local_currency_charge_back_tran_amount]        DECIMAL (14, 2) NULL,
    [usd_charge_back_tran_amount]                   DECIMAL (14, 4) NULL,
    [local_currency_code]                           VARCHAR (3)     NULL,
    [plan_rate]                                     DECIMAL (14, 4) NULL,
    [reporting_currency_code]                       VARCHAR (3)     NULL,
    [eft_return_eft_amount]                         DECIMAL (14, 4) NULL,
    [membership_current_balance]                    DECIMAL (14, 4) NULL,
    [local_currency_eft_return_eft_amount]          DECIMAL (14, 2) NULL,
    [local_currency_membership_current_balance]     DECIMAL (14, 2) NULL,
    [usd_eft_return_eft_amount]                     DECIMAL (14, 4) NULL,
    [usd_membership_current_balance]                DECIMAL (14, 4) NULL,
    [header_return_type]                            VARCHAR (50)    NULL,
    [header_date_range]                             VARCHAR (100)   NULL,
    [report_run_date_time]                          VARCHAR (50)    NULL,
    [jan_one]                                       DATETIME        NULL,
    [dv_load_date_time]                             DATETIME        NOT NULL,
    [dv_r_load_source_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                         DATETIME        NOT NULL,
    [dv_insert_user]                                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                          DATETIME        NULL,
    [dv_update_user]                                VARCHAR (50)    NULL,
    [dv_hash]                                       CHAR (32)       NOT NULL,
    [dv_batch_id]                                   BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_ACH_charge_back_detail]([dv_batch_id] ASC);

