CREATE TABLE [dbo].[d_mms_guest] (
    [d_mms_guest_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [dim_club_guest_key]    CHAR (32)    NULL,
    [guest_id]              INT          NULL,
    [first_name]            VARCHAR (50) NULL,
    [last_name]             VARCHAR (50) NULL,
    [p_mms_guest_id]        BIGINT       NOT NULL,
    [dv_load_date_time]     DATETIME     NULL,
    [dv_load_end_date_time] DATETIME     NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_guest]([dv_batch_id] ASC);

