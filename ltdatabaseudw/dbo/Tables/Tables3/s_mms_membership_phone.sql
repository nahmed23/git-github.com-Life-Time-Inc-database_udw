CREATE TABLE [dbo].[s_mms_membership_phone] (
    [s_mms_membership_phone_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [membership_phone_id]       INT          NULL,
    [area_code]                 VARCHAR (3)  NULL,
    [number]                    VARCHAR (7)  NULL,
    [inserted_date_time]        DATETIME     NULL,
    [updated_date_time]         DATETIME     NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_phone]
    ON [dbo].[s_mms_membership_phone]([bk_hash] ASC, [s_mms_membership_phone_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_phone]([dv_batch_id] ASC);

