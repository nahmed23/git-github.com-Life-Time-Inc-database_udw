CREATE TABLE [dbo].[wrk_pega_guest_club_usage] (
    [wrk_pega_guest_club_usage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [check_in_date_time]           VARCHAR (16) NULL,
    [club_id]                      INT          NULL,
    [guest_id]                     INT          NULL,
    [guest_of_dim_mms_member_key]  VARCHAR (32) NULL,
    [guest_of_member_id]           INT          NULL,
    [guest_privilege_rule_id]      INT          NULL,
    [guest_visit_id]               INT          NULL,
    [max_number_of_guests]         INT          NULL,
    [membership_id]                INT          NULL,
    [sequence_number]              INT          NULL,
    [dv_load_date_time]            DATETIME     NULL,
    [dv_load_end_date_time]        DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

