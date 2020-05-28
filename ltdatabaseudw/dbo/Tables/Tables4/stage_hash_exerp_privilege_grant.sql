CREATE TABLE [dbo].[stage_hash_exerp_privilege_grant] (
    [stage_hash_exerp_privilege_grant_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [id]                                  INT            NULL,
    [source_type]                         VARCHAR (4000) NULL,
    [source_id]                           VARCHAR (4000) NULL,
    [privilege_set_id]                    INT            NULL,
    [dummy_modified_date_time]            DATETIME       NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

