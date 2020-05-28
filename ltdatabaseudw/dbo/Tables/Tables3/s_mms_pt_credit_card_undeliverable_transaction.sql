CREATE TABLE [dbo].[s_mms_pt_credit_card_undeliverable_transaction] (
    [s_mms_pt_credit_card_undeliverable_transaction_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [pt_credit_card_undeliverable_transaction_id]       INT             NULL,
    [account_number]                                    VARCHAR (19)    NULL,
    [expiration_date]                                   DATETIME        NULL,
    [tran_amount]                                       NUMERIC (10, 3) NULL,
    [reference_code]                                    VARCHAR (6)     NULL,
    [tip_amount]                                        NUMERIC (10, 3) NULL,
    [card_holder_street_address]                        VARCHAR (20)    NULL,
    [card_holder_zip_code]                              VARCHAR (9)     NULL,
    [transaction_date_time]                             DATETIME        NULL,
    [utc_transaction_date_time]                         DATETIME        NULL,
    [transaction_date_time_zone]                        VARCHAR (4)     NULL,
    [industry_code]                                     SMALLINT        NULL,
    [reason_message]                                    VARCHAR (260)   NULL,
    [card_type]                                         VARCHAR (3)     NULL,
    [card_on_file_flag]                                 BIT             NULL,
    [inserted_date_time]                                DATETIME        NULL,
    [masked_account_number]                             VARCHAR (17)    NULL,
    [updated_date_time]                                 DATETIME        NULL,
    [masked_account_number64]                           VARCHAR (17)    NULL,
    [card_holder_name]                                  VARCHAR (50)    NULL,
    [type_indicator]                                    INT             NULL,
    [hbc_payment_flag]                                  BIT             NULL,
    [dv_load_date_time]                                 DATETIME        NOT NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL,
    [dv_r_load_source_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL,
    [dv_hash]                                           CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_pt_credit_card_undeliverable_transaction]
    ON [dbo].[s_mms_pt_credit_card_undeliverable_transaction]([bk_hash] ASC, [s_mms_pt_credit_card_undeliverable_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_pt_credit_card_undeliverable_transaction]([dv_batch_id] ASC);

