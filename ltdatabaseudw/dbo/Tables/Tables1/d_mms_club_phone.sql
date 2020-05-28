CREATE TABLE [dbo].[d_mms_club_phone] (
    [d_mms_club_phone_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [dim_mms_club_phone_key] CHAR (32)    NULL,
    [club_phone_id]          INT          NULL,
    [club_id]                INT          NULL,
    [dim_club_key]           CHAR (32)    NULL,
    [phone_number]           VARCHAR (10) NULL,
    [val_phone_type_id]      INT          NULL,
    [p_mms_club_phone_id]    BIGINT       NOT NULL,
    [dv_load_date_time]      DATETIME     NULL,
    [dv_load_end_date_time]  DATETIME     NULL,
    [dv_batch_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_club_phone]([dv_batch_id] ASC);

