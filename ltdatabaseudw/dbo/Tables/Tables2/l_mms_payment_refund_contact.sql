CREATE TABLE [dbo].[l_mms_payment_refund_contact] (
    [l_mms_payment_refund_contact_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [payment_refund_contact_id]       INT          NULL,
    [val_country_id]                  TINYINT      NULL,
    [val_state_id]                    SMALLINT     NULL,
    [payment_refund_id]               INT          NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_batch_id]                     BIGINT       NOT NULL,
    [dv_r_load_source_id]             BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_hash]                         CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_payment_refund_contact]
    ON [dbo].[l_mms_payment_refund_contact]([bk_hash] ASC, [l_mms_payment_refund_contact_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_payment_refund_contact]([dv_batch_id] ASC);

