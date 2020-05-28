CREATE TABLE [dbo].[d_mms_membership_communication_preference] (
    [d_mms_membership_communication_preference_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [membership_communication_preference_id]       INT           NULL,
    [active_flag]                                  CHAR (1)      NULL,
    [communication_preference_dim_description_key] VARCHAR (255) NULL,
    [dim_mms_membership_key]                       VARCHAR (32)  NULL,
    [inserted_date_time]                           DATETIME      NULL,
    [inserted_dim_date_key]                        VARCHAR (8)   NULL,
    [inserted_dim_time_key]                        INT           NULL,
    [membership_id]                                INT           NULL,
    [updated_date_time]                            DATETIME      NULL,
    [updated_dim_date_key]                         VARCHAR (8)   NULL,
    [updated_dim_time_key]                         INT           NULL,
    [val_communication_preference_id]              TINYINT       NULL,
    [p_mms_membership_communication_preference_id] BIGINT        NOT NULL,
    [deleted_flag]                                 INT           NULL,
    [dv_load_date_time]                            DATETIME      NULL,
    [dv_load_end_date_time]                        DATETIME      NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

