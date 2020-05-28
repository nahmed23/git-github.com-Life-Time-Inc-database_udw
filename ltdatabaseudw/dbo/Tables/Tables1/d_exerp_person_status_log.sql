CREATE TABLE [dbo].[d_exerp_person_status_log] (
    [d_exerp_person_status_log_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [person_status_log_id]         INT            NULL,
    [center_id]                    INT            NULL,
    [d_exerp_center_bk_hash]       CHAR (32)      NULL,
    [d_exerp_person_bk_hash]       CHAR (32)      NULL,
    [dim_mms_member_key]           VARCHAR (32)   NULL,
    [ets]                          BIGINT         NULL,
    [from_dim_date_key]            CHAR (8)       NULL,
    [from_dim_time_key]            CHAR (8)       NULL,
    [person_id]                    VARCHAR (4000) NULL,
    [person_status]                VARCHAR (4000) NULL,
    [p_exerp_person_status_log_id] BIGINT         NOT NULL,
    [deleted_flag]                 INT            NULL,
    [dv_load_date_time]            DATETIME       NULL,
    [dv_load_end_date_time]        DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

