CREATE TABLE [dbo].[s_mms_membership_message] (
    [s_mms_membership_message_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [membership_message_id]       INT            NULL,
    [open_date_time]              DATETIME       NULL,
    [close_date_time]             DATETIME       NULL,
    [received_date_time]          DATETIME       NULL,
    [comment]                     VARCHAR (2000) NULL,
    [utc_open_date_time]          DATETIME       NULL,
    [open_date_time_zone]         VARCHAR (4)    NULL,
    [utc_close_date_time]         DATETIME       NULL,
    [close_date_time_zone]        VARCHAR (4)    NULL,
    [utc_received_date_time]      DATETIME       NULL,
    [received_date_time_zone]     VARCHAR (4)    NULL,
    [inserted_date_time]          DATETIME       NULL,
    [updated_date_time]           DATETIME       NULL,
    [dv_load_date_time]           DATETIME       NOT NULL,
    [dv_batch_id]                 BIGINT         NOT NULL,
    [dv_r_load_source_id]         BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL,
    [dv_hash]                     CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_message]
    ON [dbo].[s_mms_membership_message]([bk_hash] ASC, [s_mms_membership_message_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_message]([dv_batch_id] ASC);

