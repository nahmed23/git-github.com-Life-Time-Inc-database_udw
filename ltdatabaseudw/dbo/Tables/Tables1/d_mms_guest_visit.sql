CREATE TABLE [dbo].[d_mms_guest_visit] (
    [d_mms_guest_visit_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)    NOT NULL,
    [fact_mms_guest_visit_key] CHAR (32)    NULL,
    [guest_visit_id]           INT          NULL,
    [check_in_dim_date_key]    CHAR (8)     NULL,
    [check_in_dim_time_key]    CHAR (8)     NULL,
    [dim_club_key]             CHAR (32)    NULL,
    [dim_mms_member_key]       CHAR (32)    NULL,
    [guest_id]                 INT          NULL,
    [member_id]                INT          NULL,
    [visit_date_time]          DATETIME     NULL,
    [p_mms_guest_visit_id]     BIGINT       NOT NULL,
    [dv_load_date_time]        DATETIME     NULL,
    [dv_load_end_date_time]    DATETIME     NULL,
    [dv_batch_id]              BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_guest_visit]([dv_batch_id] ASC);

