CREATE TABLE [dbo].[fact_mms_guest_club_usage] (
    [fact_mms_guest_club_usage_id]                BIGINT       IDENTITY (1, 1) NOT NULL,
    [fact_mms_guest_club_usage_key]               CHAR (32)    NULL,
    [guest_visit_id]                              INT          NULL,
    [check_in_dim_date_key]                       CHAR (32)    NULL,
    [check_in_dim_time_key]                       CHAR (32)    NULL,
    [dim_club_guest_key]                          CHAR (32)    NULL,
    [dim_club_key]                                CHAR (32)    NULL,
    [dim_mms_membership_guest_privilege_rule_key] CHAR (32)    NULL,
    [guest_of_dim_mms_member_key]                 CHAR (32)    NULL,
    [membership_id]                               INT          NULL,
    [dv_load_date_time]                           DATETIME     NULL,
    [dv_load_end_date_time]                       DATETIME     NULL,
    [dv_batch_id]                                 BIGINT       NOT NULL,
    [dv_inserted_date_time]                       DATETIME     NOT NULL,
    [dv_insert_user]                              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                        DATETIME     NULL,
    [dv_update_user]                              VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_guest_club_usage_key]));

