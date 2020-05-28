CREATE TABLE [dbo].[d_exerp_age_group] (
    [d_exerp_age_group_id]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)      NOT NULL,
    [dim_exerp_age_group_key] VARCHAR (32)   NULL,
    [age_group_id]            INT            NULL,
    [age_group_name]          VARCHAR (4000) NULL,
    [age_group_state]         VARCHAR (4000) NULL,
    [external_id]             VARCHAR (4000) NULL,
    [maximum_age]             INT            NULL,
    [minimum_age]             INT            NULL,
    [strict_age_limit]        INT            NULL,
    [p_exerp_age_group_id]    BIGINT         NOT NULL,
    [deleted_flag]            INT            NULL,
    [dv_load_date_time]       DATETIME       NULL,
    [dv_load_end_date_time]   DATETIME       NULL,
    [dv_batch_id]             BIGINT         NOT NULL,
    [dv_inserted_date_time]   DATETIME       NOT NULL,
    [dv_insert_user]          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]    DATETIME       NULL,
    [dv_update_user]          VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

