CREATE TABLE [dbo].[d_mms_pt_credit_card_batch] (
    [d_mms_pt_credit_card_batch_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [fact_mms_pt_credit_card_batch_key]    CHAR (32)    NULL,
    [pt_credit_card_batch_id]              INT          NULL,
    [batch_close_date_time]                DATETIME     NULL,
    [batch_close_dim_date_key]             CHAR (8)     NULL,
    [batch_close_dim_time_key]             CHAR (5)     NULL,
    [batch_closed_flag]                    CHAR (1)     NULL,
    [batch_open_date_time]                 DATETIME     NULL,
    [batch_open_dim_date_key]              CHAR (8)     NULL,
    [batch_open_dim_time_key]              CHAR (5)     NULL,
    [batch_submit_date_time]               DATETIME     NULL,
    [batch_submit_dim_date_key]            CHAR (8)     NULL,
    [batch_submit_dim_time_key]            CHAR (5)     NULL,
    [dim_mms_drawer_activity_key]          CHAR (32)    NULL,
    [fact_mms_pt_credit_card_terminal_key] CHAR (32)    NULL,
    [p_mms_pt_credit_card_batch_id]        BIGINT       NOT NULL,
    [dv_load_date_time]                    DATETIME     NULL,
    [dv_load_end_date_time]                DATETIME     NULL,
    [dv_batch_id]                          BIGINT       NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_pt_credit_card_batch]([dv_batch_id] ASC);

