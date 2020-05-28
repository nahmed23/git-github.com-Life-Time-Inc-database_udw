CREATE TABLE [dbo].[wrk_pega_child_center_usage] (
    [wrk_pega_child_center_usage_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [check_in_date]                   DATETIME     NULL,
    [check_in_member_id]              INT          NULL,
    [check_in_time]                   CHAR (5)     NULL,
    [check_out_date]                  DATETIME     NULL,
    [check_out_member_id]             INT          NULL,
    [check_out_time]                  CHAR (5)     NULL,
    [child_age_months]                INT          NULL,
    [child_center_usage_id]           INT          NULL,
    [child_member_id]                 INT          NULL,
    [club_id]                         INT          NULL,
    [fact_mms_child_center_usage_key] VARCHAR (32) NULL,
    [membership_id]                   INT          NULL,
    [sequence_number]                 INT          NULL,
    [dv_load_date_time]               DATETIME     NULL,
    [dv_load_end_date_time]           DATETIME     NULL,
    [dv_batch_id]                     BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

