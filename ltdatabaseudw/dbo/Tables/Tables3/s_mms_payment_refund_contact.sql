CREATE TABLE [dbo].[s_mms_payment_refund_contact] (
    [s_mms_payment_refund_contact_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [payment_refund_contact_id]       INT          NULL,
    [first_name]                      VARCHAR (50) NULL,
    [last_name]                       VARCHAR (50) NULL,
    [middle_init]                     CHAR (1)     NULL,
    [phone_area_code]                 VARCHAR (3)  NULL,
    [phone_number]                    VARCHAR (7)  NULL,
    [address_line1]                   VARCHAR (50) NULL,
    [address_line2]                   VARCHAR (50) NULL,
    [city]                            VARCHAR (50) NULL,
    [zip]                             VARCHAR (11) NULL,
    [inserted_date_time]              DATETIME     NULL,
    [updated_date_time]               DATETIME     NULL,
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
CREATE CLUSTERED INDEX [ci_s_mms_payment_refund_contact]
    ON [dbo].[s_mms_payment_refund_contact]([bk_hash] ASC, [s_mms_payment_refund_contact_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_payment_refund_contact]([dv_batch_id] ASC);

