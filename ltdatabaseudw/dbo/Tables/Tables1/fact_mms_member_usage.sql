CREATE TABLE [dbo].[fact_mms_member_usage] (
    [fact_mms_member_usage_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [fact_mms_member_usage_key]          CHAR (32)      NULL,
    [member_usage_id]                    INT            NULL,
    [check_in_dim_date_key]              CHAR (8)       NULL,
    [check_in_dim_date_time]             DATETIME       NULL,
    [check_in_dim_time_key]              CHAR (8)       NULL,
    [delinquent_checkin_flag]            CHAR (1)       NULL,
    [department_dim_mms_description_key] NVARCHAR (100) NULL,
    [dim_club_key]                       CHAR (32)      NULL,
    [dim_mms_checkin_member_key]         CHAR (32)      NULL,
    [dim_mms_membership_key]             CHAR (32)      NULL,
    [dim_mms_primary_member_key]         CHAR (32)      NULL,
    [gender_abbreviation]                CHAR (1)       NULL,
    [member_age_years]                   INT            NULL,
    [p_mms_member_usage_id]              BIGINT         NULL,
    [dv_load_date_time]                  DATETIME       NULL,
    [dv_load_end_date_time]              DATETIME       NULL,
    [dv_batch_id]                        BIGINT         NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_mms_member_usage_key]));

