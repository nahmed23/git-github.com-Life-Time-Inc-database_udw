CREATE TABLE [dbo].[s_mms_third_party_pos_payment] (
    [s_mms_third_party_pos_payment_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)    NOT NULL,
    [third_party_pos_payment_id]       INT          NULL,
    [offline_auth_flag]                BIT          NULL,
    [ltf_tran_date_time]               DATETIME     NULL,
    [utc_ltf_tran_date_time]           DATETIME     NULL,
    [ltf_tran_date_time_zone]          VARCHAR (4)  NULL,
    [pos_tran_date_time]               DATETIME     NULL,
    [utc_pos_tran_date_time]           DATETIME     NULL,
    [pos_tran_date_time_zone]          VARCHAR (4)  NULL,
    [pos_unique_tran_id_label]         VARCHAR (25) NULL,
    [inserted_date_time]               DATETIME     NULL,
    [updated_date_time]                DATETIME     NULL,
    [dv_load_date_time]                DATETIME     NOT NULL,
    [dv_batch_id]                      BIGINT       NOT NULL,
    [dv_r_load_source_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL,
    [dv_hash]                          CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_third_party_pos_payment]
    ON [dbo].[s_mms_third_party_pos_payment]([bk_hash] ASC, [s_mms_third_party_pos_payment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_third_party_pos_payment]([dv_batch_id] ASC);

