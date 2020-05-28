CREATE TABLE [dbo].[d_mms_pt_credit_card_transaction] (
    [d_mms_pt_credit_card_transaction_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [fact_mms_pt_credit_card_transaction_key] CHAR (32)       NULL,
    [pt_credit_card_transaction_id]           INT             NULL,
    [authorization_code]                      VARCHAR (6)     NULL,
    [card_type]                               VARCHAR (3)     NULL,
    [credit_card_last_four_digits]            VARCHAR (4)     NULL,
    [dim_mms_member_key]                      CHAR (32)       NULL,
    [fact_mms_payment_key]                    CHAR (32)       NULL,
    [fact_mms_pt_credit_card_batch_key]       CHAR (32)       NULL,
    [masked_account_number]                   VARCHAR (17)    NULL,
    [transaction_amount]                      NUMERIC (10, 3) NULL,
    [transaction_code]                        INT             NULL,
    [transaction_date_time]                   DATETIME        NULL,
    [transaction_dim_date_key]                CHAR (8)        NULL,
    [transaction_dim_time_key]                CHAR (5)        NULL,
    [voided_flag]                             CHAR (1)        NULL,
    [p_mms_pt_credit_card_transaction_id]     BIGINT          NOT NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_pt_credit_card_transaction]([dv_batch_id] ASC);

