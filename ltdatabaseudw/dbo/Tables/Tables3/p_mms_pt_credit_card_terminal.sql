CREATE TABLE [dbo].[p_mms_pt_credit_card_terminal] (
    [p_mms_pt_credit_card_terminal_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [pt_credit_card_terminal_id]           INT          NULL,
    [l_mms_pt_credit_card_terminal_id]     BIGINT       NULL,
    [s_mms_pt_credit_card_terminal_id]     BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_mms_pt_credit_card_terminal]
    ON [dbo].[p_mms_pt_credit_card_terminal]([bk_hash] ASC, [p_mms_pt_credit_card_terminal_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_mms_pt_credit_card_terminal]([dv_batch_id] ASC);

