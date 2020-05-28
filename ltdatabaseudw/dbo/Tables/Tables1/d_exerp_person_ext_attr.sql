CREATE TABLE [dbo].[d_exerp_person_ext_attr] (
    [d_exerp_person_ext_attr_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [person_ext_attr_name]       VARCHAR (4000) NULL,
    [d_exerp_center_bk_hash]     CHAR (32)      NULL,
    [d_exerp_person_bk_hash]     CHAR (32)      NULL,
    [dim_mms_member_key]         VARCHAR (32)   NULL,
    [ets]                        BIGINT         NULL,
    [person_ext_attr_value]      VARCHAR (4000) NULL,
    [p_exerp_person_ext_attr_id] BIGINT         NOT NULL,
    [deleted_flag]               INT            NULL,
    [dv_load_date_time]          DATETIME       NULL,
    [dv_load_end_date_time]      DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

