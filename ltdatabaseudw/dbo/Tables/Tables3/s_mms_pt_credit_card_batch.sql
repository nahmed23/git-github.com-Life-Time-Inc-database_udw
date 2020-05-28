CREATE TABLE [dbo].[s_mms_pt_credit_card_batch] (
    [s_mms_pt_credit_card_batch_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)       NOT NULL,
    [pt_credit_card_batch_id]       INT             NULL,
    [batch_number]                  SMALLINT        NULL,
    [transaction_count]             INT             NULL,
    [net_amount]                    DECIMAL (10, 3) NULL,
    [action_code]                   VARCHAR (2)     NULL,
    [response_code]                 VARCHAR (6)     NULL,
    [response_message]              VARCHAR (50)    NULL,
    [open_date_time]                DATETIME        NULL,
    [utc_open_date_time]            DATETIME        NULL,
    [open_date_time_zone]           VARCHAR (4)     NULL,
    [close_date_time]               DATETIME        NULL,
    [utc_close_date_time]           DATETIME        NULL,
    [close_date_time_zone]          VARCHAR (4)     NULL,
    [submit_date_time]              DATETIME        NULL,
    [utc_submit_date_time]          DATETIME        NULL,
    [submit_date_time_zone]         VARCHAR (4)     NULL,
    [inserted_date_time]            DATETIME        NULL,
    [updated_date_time]             DATETIME        NULL,
    [dv_load_date_time]             DATETIME        NOT NULL,
    [dv_batch_id]                   BIGINT          NOT NULL,
    [dv_r_load_source_id]           BIGINT          NOT NULL,
    [dv_inserted_date_time]         DATETIME        NOT NULL,
    [dv_insert_user]                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]          DATETIME        NULL,
    [dv_update_user]                VARCHAR (50)    NULL,
    [dv_hash]                       CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_pt_credit_card_batch]
    ON [dbo].[s_mms_pt_credit_card_batch]([bk_hash] ASC, [s_mms_pt_credit_card_batch_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_pt_credit_card_batch]([dv_batch_id] ASC);

