CREATE TABLE [dbo].[d_mms_membership_phone] (
    [d_mms_membership_phone_id]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)     NOT NULL,
    [dim_mms_membership_phone_key]   CHAR (32)     NULL,
    [membership_phone_id]            INT           NULL,
    [area_code]                      VARCHAR (150) NULL,
    [dim_mms_membership_key]         CHAR (32)     NULL,
    [number]                         VARCHAR (150) NULL,
    [phone_type_dim_description_key] VARCHAR (255) NULL,
    [p_mms_membership_phone_id]      BIGINT        NOT NULL,
    [deleted_flag]                   INT           NULL,
    [dv_load_date_time]              DATETIME      NULL,
    [dv_load_end_date_time]          DATETIME      NULL,
    [dv_batch_id]                    BIGINT        NOT NULL,
    [dv_inserted_date_time]          DATETIME      NOT NULL,
    [dv_insert_user]                 VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]           DATETIME      NULL,
    [dv_update_user]                 VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

