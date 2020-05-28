CREATE TABLE [dbo].[p_mms_pt_credit_card_undeliverable_transaction] (
    [p_mms_pt_credit_card_undeliverable_transaction_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)    NOT NULL,
    [pt_credit_card_undeliverable_transaction_id]       INT          NULL,
    [l_mms_pt_credit_card_undeliverable_transaction_id] BIGINT       NULL,
    [s_mms_pt_credit_card_undeliverable_transaction_id] BIGINT       NULL,
    [dv_greatest_satellite_date_time]                   DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]              DATETIME     NULL,
    [dv_load_date_time]                                 DATETIME     NOT NULL,
    [dv_load_end_date_time]                             DATETIME     NOT NULL,
    [dv_batch_id]                                       BIGINT       NOT NULL,
    [dv_inserted_date_time]                             DATETIME     NOT NULL,
    [dv_insert_user]                                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                              DATETIME     NULL,
    [dv_update_user]                                    VARCHAR (50) NULL,
    [dv_first_in_key_series]                            BIT          NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_mms_pt_credit_card_undeliverable_transaction]
    ON [dbo].[p_mms_pt_credit_card_undeliverable_transaction]([bk_hash] ASC, [p_mms_pt_credit_card_undeliverable_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_mms_pt_credit_card_undeliverable_transaction]([dv_batch_id] ASC);

