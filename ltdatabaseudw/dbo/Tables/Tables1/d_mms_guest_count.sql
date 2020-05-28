CREATE TABLE [dbo].[d_mms_guest_count] (
    [d_mms_guest_count_id]          BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [d_mms_guest_count_key]         CHAR (32)    NULL,
    [guest_count_id]                INT          NULL,
    [club_id]                       INT          NULL,
    [dim_club_key]                  CHAR (32)    NULL,
    [fact_guest_count_dim_date_key] CHAR (8)     NULL,
    [guest_count_date]              DATETIME     NULL,
    [inserted_date_time]            DATETIME     NULL,
    [inserted_dim_date_key]         VARCHAR (8)  NULL,
    [member_child_count]            INT          NULL,
    [member_count]                  INT          NULL,
    [non_member_child_count]        INT          NULL,
    [non_member_count]              INT          NULL,
    [p_mms_guest_count_id]          BIGINT       NOT NULL,
    [deleted_flag]                  INT          NULL,
    [dv_load_date_time]             DATETIME     NULL,
    [dv_load_end_date_time]         DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

