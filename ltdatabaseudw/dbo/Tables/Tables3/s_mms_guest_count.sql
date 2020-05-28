CREATE TABLE [dbo].[s_mms_guest_count] (
    [s_mms_guest_count_id]   BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [guest_count_id]         INT          NULL,
    [guest_count_date]       DATETIME     NULL,
    [member_count]           INT          NULL,
    [non_member_count]       INT          NULL,
    [member_child_count]     INT          NULL,
    [non_member_child_count] INT          NULL,
    [inserted_date_time]     DATETIME     NULL,
    [updated_date_time]      DATETIME     NULL,
    [dv_load_date_time]      DATETIME     NOT NULL,
    [dv_batch_id]            BIGINT       NOT NULL,
    [dv_r_load_source_id]    BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL,
    [dv_hash]                CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_guest_count]
    ON [dbo].[s_mms_guest_count]([bk_hash] ASC, [s_mms_guest_count_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_guest_count]([dv_batch_id] ASC);

