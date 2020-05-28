CREATE TABLE [dbo].[l_mms_pt_stored_value_card_transaction] (
    [l_mms_pt_stored_value_card_transaction_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)    NOT NULL,
    [pt_stored_value_card_transaction_id]       INT          NULL,
    [payment_id]                                INT          NULL,
    [employee_id]                               INT          NULL,
    [tip_line_parent_sv_card_transaction_id]    INT          NULL,
    [response_authorizing_network_id]           INT          NULL,
    [pt_credit_card_batch_id]                   INT          NULL,
    [dv_load_date_time]                         DATETIME     NOT NULL,
    [dv_batch_id]                               BIGINT       NOT NULL,
    [dv_r_load_source_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]                     DATETIME     NOT NULL,
    [dv_insert_user]                            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                      DATETIME     NULL,
    [dv_update_user]                            VARCHAR (50) NULL,
    [dv_hash]                                   CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_pt_stored_value_card_transaction]
    ON [dbo].[l_mms_pt_stored_value_card_transaction]([bk_hash] ASC, [l_mms_pt_stored_value_card_transaction_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_pt_stored_value_card_transaction]([dv_batch_id] ASC);

