CREATE TABLE [dbo].[d_mms_guest_privilege_rule] (
    [d_mms_guest_privilege_rule_id]               BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)     NOT NULL,
    [dim_mms_membership_guest_privilege_rule_key] CHAR (32)     NULL,
    [guest_privilege_rule_id]                     BIGINT        NULL,
    [card_level_dim_description_key]              VARCHAR (255) NULL,
    [earliest_membership_created_dim_date_key]    CHAR (32)     NULL,
    [high_membership_type_check_in_group_level]   INT           NULL,
    [latest_membership_created_dim_date_key]      CHAR (32)     NULL,
    [low_membership_type_check_in_group_level]    INT           NULL,
    [max_number_of_guests]                        INT           NULL,
    [month_flag]                                  CHAR (1)      NULL,
    [val_card_level_id]                           INT           NULL,
    [year_flag]                                   CHAR (1)      NULL,
    [p_mms_guest_privilege_rule_id]               BIGINT        NOT NULL,
    [deleted_flag]                                INT           NULL,
    [dv_load_date_time]                           DATETIME      NULL,
    [dv_load_end_date_time]                       DATETIME      NULL,
    [dv_batch_id]                                 BIGINT        NOT NULL,
    [dv_inserted_date_time]                       DATETIME      NOT NULL,
    [dv_insert_user]                              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                        DATETIME      NULL,
    [dv_update_user]                              VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

