CREATE TABLE [dbo].[s_mms_pt_stored_value_card_transaction] (
    [s_mms_pt_stored_value_card_transaction_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [pt_stored_value_card_transaction_id]       INT             NULL,
    [tran_sequence_number]                      INT             NULL,
    [transaction_code]                          INT             NULL,
    [entry_data_source]                         INT             NULL,
    [pin_capability_code]                       INT             NULL,
    [masked_account_number]                     VARCHAR (19)    NULL,
    [expiration_date]                           DATETIME        NULL,
    [tran_amount]                               DECIMAL (26, 6) NULL,
    [counter_tip_amount]                        DECIMAL (26, 6) NULL,
    [prior_approved_auth_code]                  CHAR (6)        NULL,
    [cash_out_yn]                               CHAR (1)        NULL,
    [partial_redemption_yn]                     CHAR (1)        NULL,
    [issuance_card_sequence_number]             INT             NULL,
    [issuance_n_cards]                          INT             NULL,
    [response_action_code]                      CHAR (1)        NULL,
    [response_authorization_code]               CHAR (6)        NULL,
    [response_retrieval_reference_number]       INT             NULL,
    [response_message]                          CHAR (32)       NULL,
    [response_trace_number]                     CHAR (8)        NULL,
    [response_authorization_source]             CHAR (1)        NULL,
    [response_sv_balance_amount]                DECIMAL (26, 6) NULL,
    [response_sv_previous_balance_amount]       DECIMAL (26, 6) NULL,
    [response_approved_amount]                  DECIMAL (26, 6) NULL,
    [response_cash_out_amount]                  DECIMAL (26, 6) NULL,
    [sv_batch_number]                           INT             NULL,
    [transaction_date_time]                     DATETIME        NULL,
    [utc_transaction_date_time]                 DATETIME        NULL,
    [transaction_date_timezone]                 VARCHAR (4)     NULL,
    [inserted_date_time]                        DATETIME        NULL,
    [retrieval_reference_number]                INT             NULL,
    [voided_flag]                               BIT             NULL,
    [updated_date_time]                         DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL,
    [dv_r_load_source_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL,
    [dv_hash]                                   CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_pt_stored_value_card_transaction]
    ON [dbo].[s_mms_pt_stored_value_card_transaction]([bk_hash] ASC, [s_mms_pt_stored_value_card_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_pt_stored_value_card_transaction]([dv_batch_id] ASC);

