CREATE TABLE [dbo].[fact_mms_child_center_usage_activity_area] (
    [fact_mms_child_center_usage_activity_area_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [activity_area_dim_description_key]             CHAR (255)   NULL,
    [check_in_dim_date_key]                         CHAR (8)     NULL,
    [check_in_dim_mms_member_key]                   CHAR (32)    NULL,
    [check_in_dim_time_key]                         CHAR (8)     NULL,
    [check_out_dim_date_key]                        CHAR (8)     NULL,
    [check_out_dim_mms_member_key]                  CHAR (32)    NULL,
    [check_out_dim_time_key]                        CHAR (8)     NULL,
    [child_center_usage_activity_area_id]           INT          NULL,
    [dim_club_key]                                  CHAR (32)    NULL,
    [dim_mms_membership_key]                        CHAR (32)    NULL,
    [fact_mms_child_center_usage_activity_area_key] CHAR (32)    NULL,
    [fact_mms_child_center_usage_key]               CHAR (32)    NULL,
    [length_of_stay_minutes]                        INT          NULL,
    [dv_load_date_time]                             DATETIME     NULL,
    [dv_load_end_date_time]                         DATETIME     NULL,
    [dv_batch_id]                                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                         DATETIME     NOT NULL,
    [dv_insert_user]                                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                          DATETIME     NULL,
    [dv_update_user]                                VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_child_center_usage_activity_area_key]));

