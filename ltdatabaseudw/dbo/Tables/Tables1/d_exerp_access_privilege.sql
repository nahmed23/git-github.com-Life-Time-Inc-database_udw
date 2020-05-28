CREATE TABLE [dbo].[d_exerp_access_privilege] (
    [d_exerp_access_privilege_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)      NOT NULL,
    [access_privilege_id]                    INT            NULL,
    [access_group_id]                        INT            NULL,
    [access_privilege_scope_id]              INT            NULL,
    [access_privilege_scope_type]            VARCHAR (4000) NULL,
    [d_exerp_access_group_bk_hash]           VARCHAR (32)   NULL,
    [d_exerp_access_privilege_scope_bk_hash] VARCHAR (32)   NULL,
    [dim_exerp_privilege_set_key]            VARCHAR (32)   NULL,
    [privilege_set_id]                       INT            NULL,
    [p_exerp_access_privilege_id]            BIGINT         NOT NULL,
    [deleted_flag]                           INT            NULL,
    [dv_load_date_time]                      DATETIME       NULL,
    [dv_load_end_date_time]                  DATETIME       NULL,
    [dv_batch_id]                            BIGINT         NOT NULL,
    [dv_inserted_date_time]                  DATETIME       NOT NULL,
    [dv_insert_user]                         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                   DATETIME       NULL,
    [dv_update_user]                         VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

