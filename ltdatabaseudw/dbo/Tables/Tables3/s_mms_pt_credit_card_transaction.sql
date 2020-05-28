CREATE TABLE [dbo].[s_mms_pt_credit_card_transaction] (
    [s_mms_pt_credit_card_transaction_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [pt_credit_card_transaction_id]       INT             NULL,
    [tran_sequence_number]                SMALLINT        NULL,
    [transaction_code]                    SMALLINT        NULL,
    [entry_data_source]                   SMALLINT        NULL,
    [account_number]                      VARCHAR (19)    NULL,
    [expiration_date]                     DATETIME        NULL,
    [tran_amount]                         NUMERIC (10, 3) NULL,
    [reference_code]                      VARCHAR (6)     NULL,
    [tip_amount]                          NUMERIC (10, 3) NULL,
    [card_holder_street_address]          VARCHAR (20)    NULL,
    [card_holder_zip_code]                VARCHAR (9)     NULL,
    [transaction_date_time]               DATETIME        NULL,
    [utc_transaction_date_time]           DATETIME        NULL,
    [transaction_date_time_zone]          VARCHAR (4)     NULL,
    [transaction_amount_changed_flag]     BIT             NULL,
    [industry_code]                       SMALLINT        NULL,
    [authorization_source]                CHAR (1)        NULL,
    [authorization_code]                  VARCHAR (6)     NULL,
    [authorization_response_message]      VARCHAR (50)    NULL,
    [card_type]                           VARCHAR (3)     NULL,
    [voided_flag]                         BIT             NULL,
    [card_on_file_flag]                   BIT             NULL,
    [inserted_date_time]                  DATETIME        NULL,
    [masked_account_number]               VARCHAR (17)    NULL,
    [updated_date_time]                   DATETIME        NULL,
    [masked_account_number_6_4]           VARCHAR (17)    NULL,
    [card_holder_name]                    VARCHAR (50)    NULL,
    [type_indicator]                      INT             NULL,
    [prepaid_transaction_indicator]       VARCHAR (1)     NULL,
    [ecommerce_goods_indicator]           CHAR (2)        NULL,
    [pos_retrieval_reference_number]      INT             NULL,
    [requested_amount]                    NUMERIC (10, 3) NULL,
    [prepaid_card_balance]                NUMERIC (10, 3) NULL,
    [invoice_number]                      INT             NULL,
    [sales_tax_amount]                    NUMERIC (9, 2)  NULL,
    [card_sub_type]                       INT             NULL,
    [hbc_payment_flag]                    BIT             NULL,
    [signature]                           VARCHAR (8000)  NULL,
    [eft_account_flag]                    BIT             NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_r_load_source_id]                 BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL,
    [dv_hash]                             CHAR (32)       NOT NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_pt_credit_card_transaction]
    ON [dbo].[s_mms_pt_credit_card_transaction]([bk_hash] ASC, [dv_load_date_time] ASC, [s_mms_pt_credit_card_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_pt_credit_card_transaction]([dv_batch_id] ASC);

