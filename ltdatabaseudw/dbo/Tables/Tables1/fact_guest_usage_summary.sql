CREATE TABLE [dbo].[fact_guest_usage_summary] (
    [fact_guest_usage_summary_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [club_id]                           INT          NULL,
    [data_received_late_flag]           CHAR (1)     NULL,
    [dim_club_key]                      VARCHAR (32) NULL,
    [fact_guest_usage_summary_key]      VARCHAR (32) NULL,
    [fact_mms_guest_count_dim_date_key] CHAR (8)     NULL,
    [guest_count_date]                  DATETIME     NULL,
    [guest_count_id]                    INT          NULL,
    [inserted_date_time]                DATETIME     NULL,
    [member_child_count]                INT          NULL,
    [member_count]                      INT          NULL,
    [non_member_child_count]            INT          NULL,
    [non_member_count]                  INT          NULL,
    [dv_load_date_time]                 DATETIME     NULL,
    [dv_load_end_date_time]             DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_guest_usage_summary_key]));

