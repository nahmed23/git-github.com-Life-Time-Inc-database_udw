CREATE TABLE [dbo].[stage_hash_exerp_area] (
    [stage_hash_exerp_area_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [id]                       INT            NULL,
    [parent_area_id]           INT            NULL,
    [name]                     VARCHAR (4000) NULL,
    [tree_name]                VARCHAR (4000) NULL,
    [blocked]                  BIT            NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

