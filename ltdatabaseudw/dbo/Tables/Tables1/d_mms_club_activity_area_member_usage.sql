CREATE TABLE [dbo].[d_mms_club_activity_area_member_usage] (
    [d_mms_club_activity_area_member_usage_id]            BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                             CHAR (32)     NOT NULL,
    [fact_mms_club_activity_area_member_usage_key]        CHAR (32)     NULL,
    [club_activity_area_member_usage_id]                  INT           NULL,
    [club_activity_area_member_usage_dim_description_key] VARCHAR (532) NULL,
    [dim_club_key]                                        CHAR (32)     NULL,
    [dim_mms_member_key]                                  CHAR (32)     NULL,
    [inserted_date_time]                                  DATETIME      NULL,
    [updated_date_time]                                   DATETIME      NULL,
    [val_activity_area_id]                                INT           NULL,
    [p_mms_club_activity_area_member_usage_id]            BIGINT        NOT NULL,
    [deleted_flag]                                        INT           NULL,
    [dv_load_date_time]                                   DATETIME      NULL,
    [dv_load_end_date_time]                               DATETIME      NULL,
    [dv_batch_id]                                         BIGINT        NOT NULL,
    [dv_inserted_date_time]                               DATETIME      NOT NULL,
    [dv_insert_user]                                      VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                                DATETIME      NULL,
    [dv_update_user]                                      VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

