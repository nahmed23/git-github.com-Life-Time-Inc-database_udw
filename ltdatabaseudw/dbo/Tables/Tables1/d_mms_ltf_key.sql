CREATE TABLE [dbo].[d_mms_ltf_key] (
    [d_mms_ltf_key_id]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)     NOT NULL,
    [ltf_key_id]            INT           NULL,
    [inserted_date_time]    DATETIME      NULL,
    [inserted_dim_date_key] CHAR (8)      NULL,
    [inserted_dim_time_key] CHAR (8)      NULL,
    [ltf_key_identifier]    NVARCHAR (50) NULL,
    [ltf_key_name]          NVARCHAR (50) NULL,
    [updated_date_time]     DATETIME      NULL,
    [updated_dim_date_key]  CHAR (8)      NULL,
    [updated_dim_time_key]  CHAR (8)      NULL,
    [p_mms_ltf_key_id]      BIGINT        NOT NULL,
    [deleted_flag]          INT           NULL,
    [dv_load_date_time]     DATETIME      NULL,
    [dv_load_end_date_time] DATETIME      NULL,
    [dv_batch_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

