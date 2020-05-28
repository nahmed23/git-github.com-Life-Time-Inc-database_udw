CREATE TABLE [dbo].[l_mms_pt_credit_card_rejected_transaction] (
    [l_mms_pt_credit_card_rejected_transaction_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)    NOT NULL,
    [pt_credit_card_rejected_transaction_id]       INT          NULL,
    [employee_id]                                  INT          NULL,
    [member_id]                                    INT          NULL,
    [authorization_network_id]                     TINYINT      NULL,
    [pt_credit_card_terminal_id]                   INT          NULL,
    [third_party_pos_payment_id]                   INT          NULL,
    [dv_load_date_time]                            DATETIME     NOT NULL,
    [dv_batch_id]                                  BIGINT       NOT NULL,
    [dv_r_load_source_id]                          BIGINT       NOT NULL,
    [dv_inserted_date_time]                        DATETIME     NOT NULL,
    [dv_insert_user]                               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                         DATETIME     NULL,
    [dv_update_user]                               VARCHAR (50) NULL,
    [dv_hash]                                      CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_pt_credit_card_rejected_transaction]
    ON [dbo].[l_mms_pt_credit_card_rejected_transaction]([bk_hash] ASC, [l_mms_pt_credit_card_rejected_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_pt_credit_card_rejected_transaction]([dv_batch_id] ASC);

